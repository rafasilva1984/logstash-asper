#!/usr/bin/env python3
"""
Worker do pipeline prodesp-rolemembers-daily (fonte: CyberArk tenant
Prodesp, /Redrock/query).

Porta o script do cliente que gerava `baserole_membros.csv` (Username;RoleName):
  1. catalogo de roles: SELECT Role.Name, Role.ID FROM Role;
  2. para CADA role, o relatorio pronto `@/lib/reports/compliance/mfa.js`
     com o parametro roleId -> membros da role (Username + perfil de MFA);
  3. dump da tabela User, pra incluir tambem quem nao tem role nenhuma.

E o MESMO dado do cyberark-prodesp-directory (quem tem qual role), por outra
regra: aquele usa POST /Roles/GetRoleMembers, so membros diretos do tipo User,
e ~8% das roles davam timeout e sumiam do snapshot em silencio. Este pipeline
e NOVO e isolado (data stream, template, ILM, .env e prefixo
PRODESP_ROLEMEMBERS_ proprios), roda lado a lado pra comparar e nao altera
nada do outro.

Regra de completude (pedido do cliente: "nao deixar nada de fora"):
- A role so FECHA quando o numero de usuarios distintos coletados == FullCount
  devolvido pelo relatorio. Senao ela e refeita, em varias passadas, com
  backoff. Nenhuma role e pulada (inclusive Everybody).
- Medido por probe (2026-10-26): o custo do relatorio e quase FIXO por chamada
  (7-30s, independente do PageSize) e ~1/4 das roles passa de 60s. Por isso:
  pagina grande (PAGE_SIZE 5000) e timeout alto (TIMEOUT_SECONDS 900).
- Roles pesadas (FullCount alto, demoradas ou que ja falharam, segundo o
  historico em role_stats.json) vao numa fila separada com paralelismo menor
  (HEAVY_WORKERS), pra nao derrubar o backend.

Checkpoint (STATE_DIR, default /var/lib/logstash/prodesp-rolemembers/):
  <STATE_DIR>/<snapshot_date>/roles.json              catalogo do dia
  <STATE_DIR>/<snapshot_date>/roles/<RoleId>/pNNNNN.json  linhas de cada pagina
  <STATE_DIR>/<snapshot_date>/roles/<RoleId>/meta.json    FullCount, status, tentativas
  <STATE_DIR>/<snapshot_date>/users/pNNNNN.json        paginas do dump de User
  <STATE_DIR>/<snapshot_date>/users/meta.json
  <STATE_DIR>/role_stats.json                          historico por role (classifica pesada/leve)
O checkpoint guarda as LINHAS, nao so "pagina feita": o input exec so entrega
o stdout ao Logstash quando o processo termina (CLAUDE.md 3.11). Se a execucao
cair, o que ela ja tinha coletado esta no disco, e a proxima execucao do mesmo
dia retoma das paginas que faltam e REEMITE tudo que ja esta no estado. O _id
deterministico transforma a reemissao em 409 (dedup benigno).
Execucoes simultaneas sao barradas por lock (flock) no STATE_DIR. Estados de
dias anteriores sao apagados apos KEEP_DAYS.

Agendamento: 1x/dia as 03:00 (hora local). Cada execucao para de buscar em
MAX_RUNTIME_MINUTES (default 1380 = 23h, termina antes do proximo disparo) e
emite o que tem. A 1a carga full roda fora do Logstash (run_full_load.sh).

Saida (NDJSON no stdout, logs no stderr, heartbeat no fim, CLAUDE.md 3.2):
  doc_type=member        um por par (role, usuario).
                         _id = sha256(member|snapshot_date|RoleId|Username)
  doc_type=user          um por usuario do dump de User, com role_count (roles
                         reais, sem contar Everybody) e has_role.
                         _id = sha256(user|snapshot_date|UserId)
  doc_type=role_summary  um por role por EXECUCAO: status (ok / incompleto /
                         erro / pendente), expected_count (FullCount),
                         collected_count, attempts.
                         _id = sha256(summary|run_id|RoleId)
  doc_type=run_summary   um por execucao, com os totais.
                         _id = sha256(run|run_id)
"""

import fcntl
import glob
import hashlib
import json
import os
import shutil
import sys
import threading
import time

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests

PIPELINE_NAME = "prodesp-rolemembers-daily"


# ---------------------------------------------------------------- config ---

def _env(name, default=None, cast=str):
    val = os.environ.get(name)
    if val is None or val == "":
        return default
    return cast(val)


P = "PRODESP_ROLEMEMBERS_"

TOKEN_URL = _env(P + "TOKEN_URL", "https://prodesp.id.cyberark.cloud/OAuth2/Token/PainelProdesp")
QUERY_URL = _env(P + "QUERY_URL", "https://prodesp.id.cyberark.cloud/Redrock/query")
CLIENT_ID = _env(P + "CLIENT_ID")
CLIENT_SECRET = _env(P + "CLIENT_SECRET")

REPORT_SCRIPT = _env(P + "REPORT_SCRIPT", "@/lib/reports/compliance/mfa.js")
PAGE_SIZE = _env(P + "PAGE_SIZE", 5000, int)
TIMEOUT_SECONDS = _env(P + "TIMEOUT_SECONDS", 3600, int)  # 1h por chamada (SEDUC/Everybody passam de 900s)
MAX_PAGES_PER_ROLE = _env(P + "MAX_PAGES_PER_ROLE", 1000, int)  # teto anti-loop (pagina que nunca esvazia)

LIGHT_WORKERS = _env(P + "LIGHT_WORKERS", 4, int)
HEAVY_WORKERS = _env(P + "HEAVY_WORKERS", 1, int)
HEAVY_MIN_COUNT = _env(P + "HEAVY_MIN_COUNT", 2000, int)       # FullCount acima disso = pesada
HEAVY_MIN_SECONDS = _env(P + "HEAVY_MIN_SECONDS", 60, float)   # chamada mais lenta que isso = pesada

MAX_RETRIES = _env(P + "MAX_RETRIES", 5, int)                  # por chamada
RETRY_BACKOFF_SECONDS = _env(P + "RETRY_BACKOFF_SECONDS", 15, int)
RETRY_BACKOFF_MAX_SECONDS = _env(P + "RETRY_BACKOFF_MAX_SECONDS", 300, int)
MAX_PASSES = _env(P + "MAX_PASSES", 10, int)                   # passadas sobre as roles nao fechadas
PASS_BACKOFF_SECONDS = _env(P + "PASS_BACKOFF_SECONDS", 60, int)
MAX_RUNTIME_MINUTES = _env(P + "MAX_RUNTIME_MINUTES", 1380, int)  # 23h: 1 disparo/dia (03:00), termina antes do proximo
PAUSE_SECONDS = _env(P + "PAUSE_SECONDS", 0.5, float)

USERS_ENABLED = _env(P + "USERS_ENABLED", "true").lower() in ("1", "true", "yes", "sim")
USERS_PAGE_SIZE = _env(P + "USERS_PAGE_SIZE", 50000, int)
USERS_TIMEOUT_SECONDS = _env(P + "USERS_TIMEOUT_SECONDS", 600, int)

STATE_DIR = _env(P + "STATE_DIR", "/var/lib/logstash/prodesp-rolemembers")
KEEP_DAYS = _env(P + "KEEP_DAYS", 3, int)
# Historico inicial de roles pesadas (medido no probe de 2026-10-26), usado so
# quando ainda nao existe <STATE_DIR>/role_stats.json -- assim a 1a carga ja
# manda as ~59 roles pesadas pra fila de baixo paralelismo.
ROLE_STATS_SEED = _env(P + "ROLE_STATS_SEED",
                       os.path.join(os.path.dirname(os.path.abspath(__file__)), "prodesp_rolemembers_role_stats.seed.json"))
# Fixa o snapshot_date (YYYY-MM-DD). Usado pelo run_full_load.sh pra todas as
# rodadas da carga full caírem no mesmo snapshot mesmo virando a meia-noite.
SNAPSHOT_DATE = _env(P + "SNAPSHOT_DATE")

# Roles que NAO contam pra role_count/has_role do doc user (os membros delas
# sao emitidos normalmente). Everybody e implicita: todo mundo e membro.
IMPLICIT_ROLES = {s.strip() for s in (_env(P + "IMPLICIT_ROLES", "Everybody") or "").split(",") if s.strip()}

# So pra teste (vazio/0 em producao). Com qualquer um deles setado, o dump de
# User e o has_role ficam parciais por definicao.
SKIP_ROLES = {s.strip() for s in (_env(P + "SKIP_ROLES", "") or "").split(",") if s.strip()}
ROLES_FILTER = {s.strip() for s in (_env(P + "ROLES_FILTER", "") or "").split(",") if s.strip()}
ROLES_MAX_COUNT = _env(P + "ROLES_MAX_COUNT", 0, int)
USERS_MAX_PAGES = _env(P + "USERS_MAX_PAGES", 0, int)

BRT_OFFSET = timedelta(hours=3)  # Brasil sem horario de verao desde 2019 -> UTC-3 fixo

# colunas do relatorio mfa.js (com espaco) -> campos do documento
REPORT_FIELDS = {
    "Identity Profile": "IdentityProfile",
    "Identity Challenge1": "IdentityChallenge1",
    "Identity Challenge2": "IdentityChallenge2",
    "Radius Profile": "RadiusProfile",
    "Radius Challenge1": "RadiusChallenge1",
    "Radius Challenge2": "RadiusChallenge2",
}


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sys.stderr.write(f"[{ts}] {msg}\n")
    sys.stderr.flush()


class Deadline(Exception):
    """Tempo da execucao esgotado: para de buscar, emite o que tem."""


class Clock:
    def __init__(self, minutes):
        self.end = time.monotonic() + minutes * 60

    def left(self):
        return self.end - time.monotonic()

    def check(self):
        if self.left() <= 0:
            raise Deadline()

    def sleep(self, seconds):
        time.sleep(max(0, min(seconds, self.left())))
        self.check()


# ------------------------------------------------------------------ state ---

def write_json_atomic(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except (OSError, ValueError) as e:
        log(f"[state] arquivo ilegivel {path} ({e}) -- ignorado")
        return default


def page_path(folder, n):
    return os.path.join(folder, f"p{n:05d}.json")


def saved_pages(folder):
    """Paginas gravadas, em ordem: {numero: linhas}."""
    out = {}
    for path in sorted(glob.glob(os.path.join(folder, "p*.json"))):
        try:
            n = int(os.path.basename(path)[1:6])
        except ValueError:
            continue
        rows = read_json(path)
        if rows is not None:
            out[n] = rows
    return out


def clear_pages(folder):
    for path in glob.glob(os.path.join(folder, "p*.json")):
        os.remove(path)


def cleanup_old_days(snapshot_date):
    limite = (datetime.strptime(snapshot_date, "%Y-%m-%d") - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    for path in glob.glob(os.path.join(STATE_DIR, "20??-??-??")):
        if os.path.basename(path) < limite:
            shutil.rmtree(path, ignore_errors=True)
            log(f"[state] removido estado antigo {path}")


# ------------------------------------------------------------------ auth ---

class TokenManager:
    """Thread-safe (roles em paralelo); refresh dentro do lock."""

    def __init__(self):
        self.token = None
        self.expiry = None
        self._lock = threading.Lock()

    def get_token(self):
        with self._lock:
            if self.token and self.expiry and datetime.now() < self.expiry:
                return self.token
            r = requests.post(
                TOKEN_URL,
                data={
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "grant_type": "client_credentials",
                    "scope": "all",
                },
                timeout=60,
            )
            if r.status_code != 200:
                raise Exception(f"Erro token: {r.status_code} - {r.text[:200]}")
            self.token = r.json()["access_token"]
            # sem expiry no payload: teto conservador; 401 forca refresh antes
            self.expiry = datetime.now() + timedelta(minutes=_env(P + "TOKEN_TTL_MINUTES", 15, int))
            return self.token

    def invalidate(self):
        with self._lock:
            self.token = None
            self.expiry = None


token_manager = TokenManager()


# --------------------------------------------------------------- query ---

TRANSIENT_HINTS = ("timeout", "timed out", "stream", "npgsql", "econnreset", "connection", "deadlock")


class PermanentError(Exception):
    pass


def post_query(body, timeout, tag, clock):
    """POST /Redrock/query com retry e backoff exponencial (CLAUDE.md 3.7).
    Devolve (Result, segundos da chamada que deu certo). 401 renova token;
    transiente retenta; permanente (sintaxe/permissao) sobe na hora."""
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        clock.check()
        t0 = time.monotonic()
        try:
            headers = {"Authorization": f"Bearer {token_manager.get_token()}",
                       "Content-Type": "application/json"}
            r = requests.post(QUERY_URL, json=body, headers=headers,
                              timeout=(30, max(30, min(timeout, clock.left()))))

            if r.status_code == 401:
                token_manager.invalidate()
                last_error = "401 token expirado"
            elif r.status_code != 200:
                text = r.text[:300]
                if r.status_code < 500 and not any(h in text.lower() for h in TRANSIENT_HINTS):
                    raise PermanentError(f"HTTP {r.status_code}: {text}")
                last_error = f"HTTP {r.status_code}: {text}"
            else:
                j = r.json()
                if j.get("success"):
                    return j.get("Result") or {}, time.monotonic() - t0
                msg = str(j.get("Message") or j.get("Exception") or j)[:300]
                if not any(h in msg.lower() for h in TRANSIENT_HINTS):
                    raise PermanentError(f"success=false: {msg}")
                last_error = f"success=false: {msg}"

        except requests.RequestException as e:
            last_error = f"{type(e).__name__}: {e}"[:300]

        log(f"{tag}: tentativa {attempt}/{MAX_RETRIES} falhou em {time.monotonic() - t0:.0f}s ({last_error})")
        if attempt < MAX_RETRIES:
            clock.sleep(min(RETRY_BACKOFF_SECONDS * 2 ** (attempt - 1), RETRY_BACKOFF_MAX_SECONDS))

    raise Exception(f"{tag} falhou apos {MAX_RETRIES} tentativas: {last_error}")


def rows_of(result):
    out = []
    for item in result.get("Results") or []:
        out.append(item.get("Row", item) if isinstance(item, dict) else {"Resultado": item})
    return out


# --------------------------------------------------------------- helpers ---

def make_doc_id(*parts):
    base = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def emit(doc):
    sys.stdout.write(json.dumps(doc, ensure_ascii=False, default=str) + "\n")


def emit_heartbeat():
    sys.stdout.write(json.dumps({"heartbeat": PIPELINE_NAME}) + "\n")
    sys.stdout.flush()


# ----------------------------------------------------------------- roles ---

ROLES_SCRIPT = """
SELECT
    Role.Name AS RoleName,
    Role.ID AS RoleId
FROM Role
ORDER BY Role.Name
"""


def load_roles(day_dir, clock):
    """Catalogo do dia: buscado uma vez e congelado no estado, pra todas as
    execucoes do mesmo snapshot trabalharem sobre a mesma lista."""
    path = os.path.join(day_dir, "roles.json")
    roles = read_json(path)
    if roles:
        log(f"[roles] catalogo do estado: {len(roles)} roles")
        return roles
    roles = []
    pagina = 1
    while True:
        result, _ = post_query({"Script": ROLES_SCRIPT,
                                "Args": {"PageNumber": pagina, "PageSize": 10000, "Caching": -1}},
                               timeout=300, tag=f"[roles] pagina {pagina}", clock=clock)
        linhas = rows_of(result)
        for row in linhas:
            if row.get("RoleId"):
                roles.append({"RoleId": str(row["RoleId"]), "RoleName": row.get("RoleName") or ""})
        if len(linhas) < 10000:
            break
        pagina += 1
    if not roles:
        raise Exception("catalogo de roles vazio")
    write_json_atomic(path, roles)
    log(f"[roles] catalogo novo: {len(roles)} roles")
    return roles


def role_dir(day_dir, role_id):
    return os.path.join(day_dir, "roles", role_id.replace("/", "_"))


def distinct_usernames(pages):
    return {row.get("Username") for rows in pages.values() for row in rows if row.get("Username")}


class RoleStats:
    """Historico por role entre dias (role_stats.json): FullCount e duracao
    da ultima chamada, e se ja falhou -- decide se a role e pesada."""

    def __init__(self):
        self.path = os.path.join(STATE_DIR, "role_stats.json")
        self.data = read_json(self.path)
        if self.data is None:
            self.data = read_json(ROLE_STATS_SEED, {}) or {}
            if self.data:
                log(f"[stats] sem historico -- usando seed {ROLE_STATS_SEED} ({len(self.data)} roles)")
        self.lock = threading.Lock()

    def heavy(self, role_id):
        s = self.data.get(role_id) or {}
        return ((s.get("full_count") or 0) >= HEAVY_MIN_COUNT
                or (s.get("max_seconds") or 0) >= HEAVY_MIN_SECONDS
                or bool(s.get("failed")))

    def update(self, role_id, **kw):
        with self.lock:
            s = self.data.setdefault(role_id, {})
            if "seconds" in kw:
                s["max_seconds"] = round(kw.pop("seconds"), 1)
            s.update(kw)

    def save(self):
        with self.lock:
            write_json_atomic(self.path, self.data)


def collect_role(role, day_dir, stats, clock):
    """Busca as paginas que faltam de UMA role, gravando cada pagina no
    disco assim que chega. Fecha (status ok) so quando distintos == FullCount.
    Se a paginacao terminou e nao bateu, apaga as paginas e marca pra refazer
    na proxima passada (a lista mudou no meio, ou o backend devolveu menos).
    Devolve o meta atualizado."""
    role_id, role_name = role["RoleId"], role["RoleName"]
    folder = role_dir(day_dir, role_id)
    meta_path = os.path.join(folder, "meta.json")
    meta = read_json(meta_path) or {"RoleId": role_id, "RoleName": role_name, "status": "pendente",
                                     "attempts": 0, "expected_count": None, "restarts": 0}
    if meta["status"] == "ok":
        return meta

    meta["attempts"] += 1
    meta["error"] = None
    write_json_atomic(meta_path, meta)
    pages = saved_pages(folder)
    # a pagina mais alta gravada pode ter vindo cheia; continua da seguinte
    pagina = (max(pages) + 1) if pages else 1
    ultima_cheia = not pages or len(pages[max(pages)]) >= PAGE_SIZE

    try:
        while ultima_cheia:
            if pagina > MAX_PAGES_PER_ROLE:
                raise Exception(f"MAX_PAGES_PER_ROLE={MAX_PAGES_PER_ROLE} atingido sem esvaziar")
            body = {
                "Script": REPORT_SCRIPT,
                "Args": {
                    "PageNumber": pagina,
                    "PageSize": PAGE_SIZE,
                    "Limit": PAGE_SIZE,
                    "SortBy": "",
                    "Caching": -1,
                    "Parameters": [{
                        "Name": "roleId",
                        "Value": role_id,
                        "Type": "Role",
                        "ColumnType": 12,
                        "DisplayValue": role_name,
                    }],
                },
            }
            result, segundos = post_query(body, TIMEOUT_SECONDS, f"[{role_name}] pagina {pagina}", clock)
            linhas = rows_of(result)
            if result.get("FullCount") is not None:
                meta["expected_count"] = int(result["FullCount"])
            write_json_atomic(page_path(folder, pagina), linhas)
            pages[pagina] = linhas
            meta["collected_count"] = len(distinct_usernames(pages))
            write_json_atomic(meta_path, meta)
            stats.update(role_id, full_count=meta["expected_count"], seconds=segundos)
            log(f"[{role_name}] pagina {pagina}: {len(linhas)} linhas em {segundos:.0f}s "
                f"(FullCount={meta['expected_count']})")

            ultima_cheia = len(linhas) >= PAGE_SIZE
            if meta["expected_count"] is not None and len(distinct_usernames(pages)) >= meta["expected_count"]:
                break
            pagina += 1
            clock.sleep(PAUSE_SECONDS)

        coletado = len(distinct_usernames(pages))
        meta["collected_count"] = coletado
        if meta["expected_count"] is None or coletado == meta["expected_count"]:
            meta["status"] = "ok"
            stats.update(role_id, failed=False)
        else:
            # paginacao terminou e nao bateu: refaz do zero na proxima passada.
            # As linhas ja vistas nao se perdem: vao pro arquivo de "extras",
            # emitidas junto (sao membros reais vistos hoje).
            meta["status"] = "incompleto"
            meta["restarts"] += 1
            extras_path = os.path.join(folder, "extras.json")
            extras = read_json(extras_path, []) or []
            vistos = {r.get("Username") for r in extras}
            for rows in pages.values():
                for r in rows:
                    if r.get("Username") and r["Username"] not in vistos:
                        extras.append(r)
                        vistos.add(r["Username"])
            write_json_atomic(extras_path, extras)
            clear_pages(folder)
            log(f"[{role_name}] INCOMPLETO: coletado={coletado} FullCount={meta['expected_count']} "
                f"-- vai refazer do zero (restart #{meta['restarts']})")

    except Deadline:
        meta["collected_count"] = len(distinct_usernames(pages))
        write_json_atomic(meta_path, meta)
        raise
    except Exception as e:
        meta["status"] = "erro"
        meta["error"] = str(e)[:2000]
        meta["collected_count"] = len(distinct_usernames(pages))
        stats.update(role_id, failed=True)
        log(f"[{role_name}] ERRO (tentativa {meta['attempts']}): {meta['error']}")

    write_json_atomic(meta_path, meta)
    return meta


def run_roles(roles, day_dir, stats, clock):
    """Passadas sobre as roles nao fechadas, ate todas fecharem, acabar
    MAX_PASSES ou o tempo. Duas filas simultaneas: leves (LIGHT_WORKERS) e
    pesadas (HEAVY_WORKERS, menor). Role que falha numa passada vira pesada."""
    metas = {}
    for r in roles:
        metas[r["RoleId"]] = read_json(os.path.join(role_dir(day_dir, r["RoleId"]), "meta.json")) or {
            "RoleId": r["RoleId"], "RoleName": r["RoleName"], "status": "pendente",
            "attempts": 0, "expected_count": None, "collected_count": 0}

    for passada in range(1, MAX_PASSES + 1):
        abertas = [r for r in roles if metas[r["RoleId"]]["status"] != "ok"]
        if not abertas:
            break
        pesadas = [r for r in abertas if stats.heavy(r["RoleId"])]
        leves = [r for r in abertas if not stats.heavy(r["RoleId"])]
        log(f"[passada {passada}/{MAX_PASSES}] {len(abertas)} role(s) abertas: "
            f"{len(leves)} leves (x{LIGHT_WORKERS}) + {len(pesadas)} pesadas (x{HEAVY_WORKERS}); "
            f"tempo restante {clock.left() / 60:.0f} min")

        estouro = threading.Event()

        def trabalho(role):
            if estouro.is_set():
                return
            try:
                metas[role["RoleId"]] = collect_role(role, day_dir, stats, clock)
            except Deadline:
                estouro.set()

        with ThreadPoolExecutor(LIGHT_WORKERS) as ex_leve, ThreadPoolExecutor(HEAVY_WORKERS) as ex_pesada:
            # pesadas primeiro na fila delas: sao as que mais demoram
            futures = [ex_pesada.submit(trabalho, r) for r in pesadas]
            futures += [ex_leve.submit(trabalho, r) for r in leves]
            for f in futures:
                f.result()
        stats.save()

        if estouro.is_set():
            log("[roles] MAX_RUNTIME_MINUTES atingido -- proxima execucao do dia retoma do checkpoint")
            break

        abertas = [r for r in roles if metas[r["RoleId"]]["status"] != "ok"]
        if abertas and passada < MAX_PASSES:
            espera = min(PASS_BACKOFF_SECONDS * passada, RETRY_BACKOFF_MAX_SECONDS)
            log(f"[passada {passada}] {len(abertas)} role(s) nao fecharam; nova passada em {espera}s")
            try:
                clock.sleep(espera)
            except Deadline:
                log("[roles] MAX_RUNTIME_MINUTES atingido durante o backoff")
                break

    return metas


# ------------------------------------------------------------------ users --

USERS_SCRIPT = """
SELECT
    User.Username,
    User.ID AS UserId,
    User.Status AS UserStatus,
    User.LastLogin
FROM
    User
ORDER BY
    User.Username
"""


def collect_users(day_dir, clock):
    """Dump paginado da tabela User com checkpoint por pagina. Devolve
    (paginas, completo)."""
    folder = os.path.join(day_dir, "users")
    meta_path = os.path.join(folder, "meta.json")
    meta = read_json(meta_path) or {"complete": False}
    pages = saved_pages(folder)
    if meta.get("complete"):
        return pages, True

    pagina = (max(pages) + 1) if pages else 1
    if pages and len(pages[max(pages)]) < USERS_PAGE_SIZE:
        meta["complete"] = True
        write_json_atomic(meta_path, meta)
        return pages, True

    while True:
        if USERS_MAX_PAGES and pagina > USERS_MAX_PAGES:
            log(f"[users] USERS_MAX_PAGES={USERS_MAX_PAGES} atingido (teste) -- dump parcial")
            return pages, False
        try:
            result, segundos = post_query(
                {"Script": USERS_SCRIPT, "Args": {"PageNumber": pagina, "PageSize": USERS_PAGE_SIZE, "Caching": -1}},
                USERS_TIMEOUT_SECONDS, f"[users] pagina {pagina}", clock)
        except Deadline:
            log("[users] MAX_RUNTIME_MINUTES atingido -- proxima execucao retoma do checkpoint")
            return pages, False
        except Exception as e:
            log(f"[users] ERRO na pagina {pagina}: {e} -- proxima execucao retoma daqui")
            return pages, False
        linhas = rows_of(result)
        write_json_atomic(page_path(folder, pagina), linhas)
        pages[pagina] = linhas
        log(f"[users] pagina {pagina}: {len(linhas)} linhas em {segundos:.0f}s "
            f"(total {sum(len(v) for v in pages.values())})")
        if len(linhas) < USERS_PAGE_SIZE:
            meta["complete"] = True
            write_json_atomic(meta_path, meta)
            return pages, True
        pagina += 1


def parse_ms_date(value):
    """'/Date(<ms>)/' -> ISO UTC; outro formato fica so no _raw."""
    if not value:
        return None
    s = str(value)
    if s.startswith("/Date(") and s.endswith(")/"):
        try:
            ms = int(s[6:-2].split("+")[0].split("-")[0])
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        except ValueError:
            return None
    return None


# ------------------------------------------------------------------ emit ---

def emit_all(roles, metas, day_dir, ctx, users_pages, users_complete):
    """Reemite TUDO que esta no estado do dia (inclusive o que execucoes
    anteriores ja coletaram -- o stdout delas pode ter se perdido). _id
    deterministico -> o repetido vira 409."""
    sd, sms = ctx["snapshot_date"], ctx["snapshot_ms"]
    roles_por_user = {}
    total_member = 0

    for r in roles:
        role_id, role_name = r["RoleId"], r["RoleName"]
        folder = role_dir(day_dir, role_id)
        linhas = [row for rows in saved_pages(folder).values() for row in rows]
        linhas += read_json(os.path.join(folder, "extras.json"), []) or []
        vistos = set()
        for row in linhas:
            username = row.get("Username")
            if not username or username in vistos:
                continue
            vistos.add(username)
            doc = {"doc_type": "member", "snapshot_date": sd, "snapshot_ms": sms,
                   "RoleId": role_id, "RoleName": role_name, "Username": username}
            for src, dst in REPORT_FIELDS.items():
                doc[dst] = row.get(src)
            doc["_doc_id"] = make_doc_id("member", sd, role_id, username)
            emit(doc)
            total_member += 1
            if role_name not in IMPLICIT_ROLES and role_id not in IMPLICIT_ROLES:
                roles_por_user[username.lower()] = roles_por_user.get(username.lower(), 0) + 1
    sys.stdout.flush()

    # O doc user tem _id por dia: se fosse emitido com role_count parcial, a
    # versao correta de uma execucao posterior viraria 409 e o errado ficaria.
    # Por isso so sai quando TODAS as roles que contam (fora IMPLICIT_ROLES,
    # que nao entram no role_count) fecharam.
    roles_completas = not (SKIP_ROLES or ROLES_FILTER or ROLES_MAX_COUNT) and all(
        m["status"] == "ok" for m in metas.values()
        if m["RoleName"] not in IMPLICIT_ROLES and m["RoleId"] not in IMPLICIT_ROLES)
    total_user = sem_role = 0
    if users_pages and not roles_completas:
        log("[users] docs user NAO emitidos nesta execucao: ainda ha role aberta (role_count seria parcial)")
        users_pages = {}
    for rows in users_pages.values():
        for row in rows:
            username = row.get("Username")
            if not username:
                continue
            n = roles_por_user.get(username.lower(), 0)
            doc = {"doc_type": "user", "snapshot_date": sd, "snapshot_ms": sms,
                   "Username": username, "UserId": row.get("UserId"), "UserStatus": row.get("UserStatus"),
                   "LastLogin": parse_ms_date(row.get("LastLogin")), "LastLogin_raw": row.get("LastLogin"),
                   "role_count": n, "has_role": n > 0, "roles_complete": roles_completas}
            doc["_doc_id"] = make_doc_id("user", sd, row.get("UserId") or username)
            emit(doc)
            total_user += 1
            sem_role += n == 0
    sys.stdout.flush()

    for m in metas.values():
        emit({"doc_type": "role_summary", "snapshot_date": sd, "snapshot_ms": sms, "run_id": ctx["run_id"],
              "RoleId": m["RoleId"], "RoleName": m["RoleName"], "status": m["status"],
              "expected_count": m.get("expected_count"), "collected_count": m.get("collected_count"),
              "attempts": m.get("attempts"), "restarts": m.get("restarts", 0), "error": m.get("error"),
              "_doc_id": make_doc_id("summary", ctx["run_id"], m["RoleId"])})

    return total_member, total_user, sem_role, roles_completas


# ------------------------------------------------------------------ main --

def run(ctx, clock):
    day_dir = os.path.join(STATE_DIR, ctx["snapshot_date"])
    os.makedirs(day_dir, exist_ok=True)
    cleanup_old_days(ctx["snapshot_date"])
    stats = RoleStats()

    roles = load_roles(day_dir, clock)
    if ROLES_FILTER:
        roles = [r for r in roles if r["RoleName"] in ROLES_FILTER or r["RoleId"] in ROLES_FILTER]
        log(f"[roles] ROLES_FILTER (teste) -> {len(roles)} roles")
    if SKIP_ROLES:
        roles = [r for r in roles if r["RoleName"] not in SKIP_ROLES and r["RoleId"] not in SKIP_ROLES]
        log(f"[roles] SKIP_ROLES (teste) -> {len(roles)} roles")
    if ROLES_MAX_COUNT:
        roles = roles[:ROLES_MAX_COUNT]
        log(f"[roles] ROLES_MAX_COUNT={ROLES_MAX_COUNT} (teste)")

    metas = run_roles(roles, day_dir, stats, clock)

    users_pages, users_complete = ({}, False)
    if USERS_ENABLED:
        users_pages, users_complete = collect_users(day_dir, clock)

    total_member, total_user, sem_role, roles_completas = emit_all(
        roles, metas, day_dir, ctx, users_pages, users_complete)

    por_status = {}
    for m in metas.values():
        por_status[m["status"]] = por_status.get(m["status"], 0) + 1
    esperado = sum(m.get("expected_count") or 0 for m in metas.values())
    coletado = sum(m.get("collected_count") or 0 for m in metas.values())
    abertas = sorted((m for m in metas.values() if m["status"] != "ok"), key=lambda m: m["RoleName"])

    emit({"doc_type": "run_summary", "snapshot_date": ctx["snapshot_date"], "snapshot_ms": ctx["snapshot_ms"],
          "run_id": ctx["run_id"], "roles_total": len(metas), "roles_ok": por_status.get("ok", 0),
          "roles_not_ok": len(abertas), "expected_count": esperado, "collected_count": coletado,
          "members_emitted": total_member, "users_emitted": total_user, "users_without_role": sem_role,
          "users_complete": users_complete, "roles_complete": roles_completas,
          "duration_s": round(time.time() - ctx["t0"], 1),
          "_doc_id": make_doc_id("run", ctx["run_id"])})

    log(f"[fim] roles={len(metas)} status={por_status} esperado(FullCount)={esperado} coletado={coletado} "
        f"| member docs={total_member} | users={total_user} (sem role={sem_role}, dump completo={users_complete})")
    for m in abertas:
        log(f"[fim] NAO FECHOU: {m['RoleName']} ({m['RoleId']}) status={m['status']} "
            f"coletado={m.get('collected_count')} < FullCount={m.get('expected_count')} "
            f"tentativas={m.get('attempts')} {m.get('error') or ''}")
    if not abertas:
        log("[fim] TODAS as roles fecharam (coletado == FullCount)")


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        log(f"{P}CLIENT_ID / {P}CLIENT_SECRET nao configurados")
        emit_heartbeat()
        return

    now_utc = datetime.now(timezone.utc)
    ctx = {
        "t0": time.time(),
        "snapshot_ms": int(now_utc.timestamp() * 1000),
        "snapshot_date": SNAPSHOT_DATE or (now_utc - BRT_OFFSET).strftime("%Y-%m-%d"),
        "run_id": now_utc.strftime("%Y%m%dT%H%M%SZ"),
    }

    os.makedirs(STATE_DIR, exist_ok=True)
    lock_file = open(os.path.join(STATE_DIR, ".lock"), "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("outra execucao ainda esta rodando (lock em STATE_DIR) -- saindo sem fazer nada")
        emit_heartbeat()
        return

    log(f"inicio run_id={ctx['run_id']} snapshot_date={ctx['snapshot_date']} state={STATE_DIR}")
    try:
        run(ctx, Clock(MAX_RUNTIME_MINUTES))
    except Exception as e:
        log(f"falha nao recuperada: {type(e).__name__}: {e}")

    emit_heartbeat()
    sys.stdout.flush()


if __name__ == "__main__":
    main()
