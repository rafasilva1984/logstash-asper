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

Duas vias por role (medido por probe em 2026-10-26/27):
- `mfa`: o relatorio do cliente. Traz as colunas de MFA, mas o custo cresce
  com o tamanho da role e NAO depende do PageSize (o relatorio calcula a role
  inteira antes de paginar). O servidor corta a chamada em ~300s (HTTP 500
  "Idaptive"), entao roles grandes (Balcao Unico 6.218, SEDUC - Tutoria
  190.688...) nunca saem por aqui, por mais retry/timeout que se de.
- `rm`: SQL na RoleMember FILTRADA pela role (`WHERE RoleMember.RoleID =
  '<id>'`), paginada: 0,5s pra 505 membros, 7s pra 10.000 linhas da SEDUC.
  Devolve MemberID (= User.ID); o Username vem do dump de User. (A tabela
  RoleMember INTEIRA, sem WHERE, trava -- incidente #1 do prodesp-directory.)
Regra: role leve vai pelo `mfa` (regra do cliente); role pesada (historico em
role_stats.json, seed do probe) ou que falhar no `mfa` vai pelo `rm` na hora,
sem gastar 5 retries de 5 min num corte que e do servidor. Todo doc leva
`source_method`. Toda role tambem ganha a contagem da RoleMember
(`rolemember_count`) no role_summary, como conferencia cruzada.

Regra de completude (pedido do cliente: "nao deixar nada de fora"):
- A role so FECHA quando o coletado == FullCount da via usada. Senao ela e
  refeita, em varias passadas, com backoff. Nenhuma role e pulada.
- Roles pesadas vao numa fila separada com paralelismo menor (HEAVY_WORKERS).

Checkpoint (STATE_DIR, default /var/lib/logstash/prodesp-rolemembers/):
  <STATE_DIR>/<snapshot_date>/roles.json                    catalogo do dia
  <STATE_DIR>/<snapshot_date>/roles/<RoleId>/pNNNNN.json    paginas da via mfa
  <STATE_DIR>/<snapshot_date>/roles/<RoleId>/rm/pNNNNN.json paginas da via rm
  <STATE_DIR>/<snapshot_date>/roles/<RoleId>/meta.json      via, FullCount, status
  <STATE_DIR>/<snapshot_date>/users/pNNNNN.json             paginas do dump de User
  <STATE_DIR>/role_stats.json                               historico (pesada/leve)
O checkpoint guarda as LINHAS, nao so "pagina feita": o input exec so entrega
o stdout ao Logstash quando o processo termina (CLAUDE.md 3.11). Se a execucao
cair, o que ela ja tinha coletado esta no disco, e a proxima execucao do mesmo
snapshot retoma das paginas que faltam e REEMITE tudo que ja esta no estado.
O _id deterministico transforma a reemissao em 409 (dedup benigno).
Execucoes simultaneas sao barradas por lock (flock) no STATE_DIR. Estados de
dias anteriores sao apagados apos KEEP_DAYS.

Ordem: dump de User primeiro (a via rm precisa dele pra resolver o Username),
depois as roles. Membros da via rm so sao emitidos com o dump de User
completo; docs user so com todas as roles fechadas (o _id e por dia: um valor
parcial emitido antes viraria 409 na versao correta e ficaria errado).

Agendamento: 1x/dia as 03:00 (hora local). Cada execucao para de buscar em
MAX_RUNTIME_MINUTES (default 1380 = 23h) e emite o que tem. A 1a carga full
roda fora do Logstash (run_full_load.sh).

Saida (NDJSON no stdout, logs no stderr, heartbeat no fim, CLAUDE.md 3.2):
  doc_type=member        um por par (role, usuario).
                         _id = sha256(member|snapshot_date|RoleId|Username)
  doc_type=user          um por usuario do dump de User, com role_count (roles
                         reais, sem contar Everybody) e has_role.
                         _id = sha256(user|snapshot_date|UserId)
  doc_type=role_summary  um por role por EXECUCAO: status, source_method,
                         expected_count, collected_count, rolemember_count.
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

# via mfa (relatorio do cliente). O servidor corta em ~300s: timeout um pouco
# acima disso e poucas tentativas -- falhou, cai pra via rm na hora.
REPORT_SCRIPT = _env(P + "REPORT_SCRIPT", "@/lib/reports/compliance/mfa.js")
MFA_PAGE_SIZE = _env(P + "MFA_PAGE_SIZE", 5000, int)
MFA_TIMEOUT_SECONDS = _env(P + "MFA_TIMEOUT_SECONDS", 330, int)
MFA_MAX_ATTEMPTS = _env(P + "MFA_MAX_ATTEMPTS", 2, int)

# via rm (RoleMember filtrada pela role) e demais queries SQL
RM_PAGE_SIZE = _env(P + "RM_PAGE_SIZE", 10000, int)
TIMEOUT_SECONDS = _env(P + "TIMEOUT_SECONDS", 330, int)
MAX_RETRIES = _env(P + "MAX_RETRIES", 5, int)
MAX_PAGES_PER_ROLE = _env(P + "MAX_PAGES_PER_ROLE", 1000, int)  # teto anti-loop

LIGHT_WORKERS = _env(P + "LIGHT_WORKERS", 4, int)
HEAVY_WORKERS = _env(P + "HEAVY_WORKERS", 1, int)
HEAVY_MIN_COUNT = _env(P + "HEAVY_MIN_COUNT", 2000, int)       # FullCount acima disso = pesada
HEAVY_MIN_SECONDS = _env(P + "HEAVY_MIN_SECONDS", 60, float)   # chamada mfa mais lenta que isso = pesada

RETRY_BACKOFF_SECONDS = _env(P + "RETRY_BACKOFF_SECONDS", 15, int)
RETRY_BACKOFF_MAX_SECONDS = _env(P + "RETRY_BACKOFF_MAX_SECONDS", 300, int)
MAX_PASSES = _env(P + "MAX_PASSES", 10, int)                   # passadas sobre as roles nao fechadas
PASS_BACKOFF_SECONDS = _env(P + "PASS_BACKOFF_SECONDS", 60, int)
MAX_RUNTIME_MINUTES = _env(P + "MAX_RUNTIME_MINUTES", 1380, int)  # 23h: 1 disparo/dia (03:00)
PAUSE_SECONDS = _env(P + "PAUSE_SECONDS", 0.5, float)

USERS_PAGE_SIZE = _env(P + "USERS_PAGE_SIZE", 50000, int)
USERS_TIMEOUT_SECONDS = _env(P + "USERS_TIMEOUT_SECONDS", 600, int)

STATE_DIR = _env(P + "STATE_DIR", "/var/lib/logstash/prodesp-rolemembers")
KEEP_DAYS = _env(P + "KEEP_DAYS", 3, int)
# Historico inicial de roles pesadas (medido no probe de 2026-10-26), usado so
# quando ainda nao existe <STATE_DIR>/role_stats.json -- assim a 1a carga ja
# manda as ~59 roles pesadas direto pra via rm, na fila de baixo paralelismo.
ROLE_STATS_SEED = _env(P + "ROLE_STATS_SEED",
                       os.path.join(os.path.dirname(os.path.abspath(__file__)), "prodesp_rolemembers_role_stats.seed.json"))
# Fixa o snapshot_date (YYYY-MM-DD). Usado pelo run_full_load.sh pra todas as
# rodadas da carga full cairem no mesmo snapshot mesmo virando a meia-noite.
SNAPSHOT_DATE = _env(P + "SNAPSHOT_DATE")

# Roles que NAO contam pra role_count/has_role do doc user (os membros delas
# sao emitidos normalmente). Everybody e implicita: todo mundo e membro.
IMPLICIT_ROLES = {s.strip() for s in (_env(P + "IMPLICIT_ROLES", "Everybody") or "").split(",") if s.strip()}

# So pra teste (vazio/0 em producao). Com qualquer um deles setado, o has_role
# fica parcial por definicao e os docs user nao saem.
SKIP_ROLES = {s.strip() for s in (_env(P + "SKIP_ROLES", "") or "").split(",") if s.strip()}
ROLES_FILTER = {s.strip() for s in (_env(P + "ROLES_FILTER", "") or "").split(",") if s.strip()}
ROLES_MAX_COUNT = _env(P + "ROLES_MAX_COUNT", 0, int)
USERS_MAX_PAGES = _env(P + "USERS_MAX_PAGES", 0, int)
FORCE_METHOD = _env(P + "FORCE_METHOD")  # teste: "mfa" ou "rm" pra todas

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


def short_error(text):
    """Corpo de erro da CyberArk vem como pagina HTML ("Idaptive"): resume."""
    t = (text or "").strip()
    if t.lower().startswith("<!doctype") or t.lower().startswith("<html"):
        return "pagina de erro HTML do servidor (corte ~300s do lado da CyberArk)"
    return t[:300]


def post_query(body, timeout, tag, clock, retries=None):
    """POST /Redrock/query com retry e backoff exponencial (CLAUDE.md 3.7).
    Devolve (Result, segundos da chamada que deu certo). 401 renova token;
    transiente retenta; permanente (sintaxe/permissao) sobe na hora."""
    retries = retries or MAX_RETRIES
    last_error = None
    for attempt in range(1, retries + 1):
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
                text = short_error(r.text)
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

        log(f"{tag}: tentativa {attempt}/{retries} falhou em {time.monotonic() - t0:.0f}s ({last_error})")
        if attempt < retries:
            clock.sleep(min(RETRY_BACKOFF_SECONDS * 2 ** (attempt - 1), RETRY_BACKOFF_MAX_SECONDS))

    raise Exception(f"{tag} falhou apos {retries} tentativas: {last_error}")


def rows_of(result):
    out = []
    for item in result.get("Results") or []:
        out.append(item.get("Row", item) if isinstance(item, dict) else {"Resultado": item})
    return out


def sql_str(value):
    return "'" + str(value).replace("'", "''") + "'"


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


def mfa_keys(pages):
    return {row.get("Username") for rows in pages.values() for row in rows if row.get("Username")}


def rm_keys(pages):
    return {row.get("MemberID") for rows in pages.values() for row in rows if row.get("MemberID")}


class RoleStats:
    """Historico por role entre dias (role_stats.json): FullCount e duracao
    da ultima chamada mfa, e se ja falhou no mfa -- decide a via e a fila."""

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


def fetch_pages(folder, meta, method, build_body, page_size, timeout, retries, keys_of, tag, clock):
    """Pagina uma via, gravando cada pagina no disco assim que chega e
    retomando das paginas ja gravadas. Para quando a pagina vem incompleta ou
    o coletado alcanca o FullCount. Devolve (pages, FullCount, max_segundos)."""
    # pagina salva com outro PageSize nao casa com a numeracao atual: recomeca
    if meta.get(f"{method}_page_size") != page_size and glob.glob(os.path.join(folder, "p*.json")):
        log(f"{tag} paginas salvas com outro PageSize ({meta.get(f'{method}_page_size')} -> {page_size}) "
            f"-- recomecando a via")
        clear_pages(folder)
        meta.pop(f"{method}_full_count", None)
    meta[f"{method}_page_size"] = page_size
    pages = saved_pages(folder)
    pagina = (max(pages) + 1) if pages else 1
    ultima_cheia = not pages or len(pages[max(pages)]) >= page_size
    full_count = meta.get(f"{method}_full_count")
    max_seg = 0.0
    if full_count is not None and len(keys_of(pages)) >= full_count:
        ultima_cheia = False

    while ultima_cheia:
        if pagina > MAX_PAGES_PER_ROLE:
            raise Exception(f"MAX_PAGES_PER_ROLE={MAX_PAGES_PER_ROLE} atingido sem esvaziar")
        result, segundos = post_query(build_body(pagina), timeout, f"{tag} pagina {pagina}", clock, retries)
        max_seg = max(max_seg, segundos)
        linhas = rows_of(result)
        if result.get("FullCount") is not None:
            full_count = int(result["FullCount"])
            meta[f"{method}_full_count"] = full_count
        write_json_atomic(page_path(folder, pagina), linhas)
        pages[pagina] = linhas
        log(f"{tag} pagina {pagina}: {len(linhas)} linhas em {segundos:.0f}s (FullCount={full_count})")
        ultima_cheia = len(linhas) >= page_size
        if full_count is not None and len(keys_of(pages)) >= full_count:
            break
        pagina += 1
        clock.sleep(PAUSE_SECONDS)

    return pages, full_count, max_seg


def mfa_body(role):
    def build(pagina):
        return {
            "Script": REPORT_SCRIPT,
            "Args": {
                "PageNumber": pagina,
                "PageSize": MFA_PAGE_SIZE,
                "Limit": MFA_PAGE_SIZE,
                "SortBy": "",
                "Caching": -1,
                "Parameters": [{
                    "Name": "roleId",
                    "Value": role["RoleId"],
                    "Type": "Role",
                    "ColumnType": 12,
                    "DisplayValue": role["RoleName"],
                }],
            },
        }
    return build


def rm_body(role):
    script = (f"SELECT RoleMember.MemberID, RoleMember.MemberType FROM RoleMember "
              f"WHERE RoleMember.RoleID = {sql_str(role['RoleId'])} ORDER BY RoleMember.MemberID")

    def build(pagina):
        return {"Script": script, "Args": {"PageNumber": pagina, "PageSize": RM_PAGE_SIZE, "Caching": -1}}
    return build


def rolemember_count(role, clock):
    """Conferencia cruzada: quantos registros a RoleMember tem pra role."""
    try:
        result, _ = post_query(rm_body(role)(1) | {"Args": {"PageNumber": 1, "PageSize": 1, "Caching": -1}},
                               TIMEOUT_SECONDS, f"[{role['RoleName']}] contagem RoleMember", clock, 2)
        return int(result["FullCount"]) if result.get("FullCount") is not None else None
    except Deadline:
        raise
    except Exception:
        return None


def collect_role(role, day_dir, stats, clock):
    """Coleta UMA role pela via da vez (mfa ou rm). Fecha (status ok) so
    quando coletado == FullCount. Se a paginacao terminou e nao bateu, apaga
    as paginas e marca pra refazer na proxima passada. Falha no mfa troca pra
    rm na hora. Devolve o meta atualizado."""
    role_id, role_name = role["RoleId"], role["RoleName"]
    folder = role_dir(day_dir, role_id)
    meta_path = os.path.join(folder, "meta.json")
    meta = read_json(meta_path) or {"RoleId": role_id, "RoleName": role_name, "status": "pendente",
                                     "attempts": 0, "restarts": 0}
    if meta["status"] == "ok":
        return meta

    meta["attempts"] = meta.get("attempts", 0) + 1
    meta.setdefault("restarts", 0)
    meta["error"] = None
    if not meta.get("method"):
        meta["method"] = FORCE_METHOD or ("rm" if stats.heavy(role_id) else "mfa")
    write_json_atomic(meta_path, meta)

    try:
        if meta.get("rolemember_count") is None:
            meta["rolemember_count"] = rolemember_count(role, clock)

        if meta["method"] == "mfa":
            try:
                pages, full_count, seg = fetch_pages(
                    folder, meta, "mfa", mfa_body(role), MFA_PAGE_SIZE, MFA_TIMEOUT_SECONDS,
                    MFA_MAX_ATTEMPTS, mfa_keys, f"[{role_name}][mfa]", clock)
                stats.update(role_id, full_count=full_count, seconds=seg)
                keys = mfa_keys(pages)
            except (Deadline, PermanentError):
                raise
            except Exception as e:
                # corte do servidor (~300s): nao adianta insistir no mfa.
                # As linhas mfa ja vistas ficam (vao pro emit como extras).
                log(f"[{role_name}] via mfa falhou ({e}) -- trocando pra via rm (RoleMember)")
                stats.update(role_id, failed=True)
                meta["mfa_error"] = str(e)[:500]
                meta["method"] = "rm"
                write_json_atomic(meta_path, meta)

        if meta["method"] == "rm":
            pages, full_count, _ = fetch_pages(
                os.path.join(folder, "rm"), meta, "rm", rm_body(role), RM_PAGE_SIZE, TIMEOUT_SECONDS,
                MAX_RETRIES, rm_keys, f"[{role_name}][rm]", clock)
            keys = rm_keys(pages)

        meta["expected_count"] = full_count
        meta["collected_count"] = len(keys)
        if full_count is None or len(keys) == full_count:
            meta["status"] = "ok"
        else:
            # paginacao terminou e nao bateu (lista mudou no meio): refaz do
            # zero na proxima passada; as linhas vistas vao pros extras e
            # tambem sao emitidas (membros reais vistos hoje).
            meta["status"] = "incompleto"
            meta["restarts"] += 1
            sub = folder if meta["method"] == "mfa" else os.path.join(folder, "rm")
            extras_path = os.path.join(sub, "extras.json")
            extras = read_json(extras_path, []) or []
            extras.extend(r for rows in pages.values() for r in rows)
            write_json_atomic(extras_path, extras)
            clear_pages(sub)
            log(f"[{role_name}] INCOMPLETO ({meta['method']}): coletado={len(keys)} FullCount={full_count} "
                f"-- vai refazer do zero (restart #{meta['restarts']})")

    except Deadline:
        write_json_atomic(meta_path, meta)
        raise
    except Exception as e:
        meta["status"] = "erro"
        meta["error"] = str(e)[:2000]
        log(f"[{role_name}] ERRO via {meta['method']} (tentativa {meta['attempts']}): {meta['error']}")

    write_json_atomic(meta_path, meta)
    return meta


def run_roles(roles, day_dir, stats, clock):
    """Passadas sobre as roles nao fechadas, ate todas fecharem, acabar
    MAX_PASSES ou o tempo. Duas filas simultaneas: leves (LIGHT_WORKERS) e
    pesadas (HEAVY_WORKERS, menor)."""
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
            futures = [ex_pesada.submit(trabalho, r) for r in pesadas]
            futures += [ex_leve.submit(trabalho, r) for r in leves]
            for f in futures:
                f.result()
        stats.save()

        if estouro.is_set():
            log("[roles] MAX_RUNTIME_MINUTES atingido -- proxima execucao do snapshot retoma do checkpoint")
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
    if (not meta.get("complete") and meta.get("page_size") != USERS_PAGE_SIZE
            and glob.glob(os.path.join(folder, "p*.json"))):
        log(f"[users] paginas salvas com outro PageSize ({meta.get('page_size')} -> {USERS_PAGE_SIZE}) "
            f"-- recomecando o dump")
        clear_pages(folder)
    if not meta.get("complete"):
        meta["page_size"] = USERS_PAGE_SIZE
    write_json_atomic(meta_path, meta)
    pages = saved_pages(folder)
    if meta.get("complete"):
        log(f"[users] dump do estado: {sum(len(v) for v in pages.values())} usuarios")
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
    """Reemite TUDO que esta no estado do snapshot (inclusive o que execucoes
    anteriores ja coletaram -- o stdout delas pode ter se perdido). _id
    deterministico -> o repetido vira 409."""
    sd, sms = ctx["snapshot_date"], ctx["snapshot_ms"]
    by_id = {}
    by_name = {}
    for rows in users_pages.values():
        for row in rows:
            if row.get("UserId"):
                by_id[str(row["UserId"]).lower()] = row
            if row.get("Username"):
                by_name[row["Username"].lower()] = row

    roles_por_user = {}
    total_member = 0
    rm_adiadas = []

    for r in roles:
        role_id, role_name = r["RoleId"], r["RoleName"]
        folder = role_dir(day_dir, role_id)
        meta = metas.get(role_id) or {}
        implicita = role_name in IMPLICIT_ROLES or role_id in IMPLICIT_ROLES
        vistos = set()

        def add(username, user_id, extra, method):
            nonlocal total_member
            key = username or f"id:{user_id}"
            if key.lower() in vistos:
                return
            vistos.add(key.lower())
            doc = {"doc_type": "member", "snapshot_date": sd, "snapshot_ms": sms,
                   "RoleId": role_id, "RoleName": role_name, "Username": username,
                   "UserId": user_id, "source_method": method}
            doc.update(extra)
            doc["_doc_id"] = make_doc_id("member", sd, role_id, key)
            emit(doc)
            total_member += 1
            if username and not implicita:
                roles_por_user[username.lower()] = roles_por_user.get(username.lower(), 0) + 1

        # via mfa: paginas + extras (mesmo de role que depois trocou pra rm)
        linhas = [row for rows in saved_pages(folder).values() for row in rows]
        linhas += read_json(os.path.join(folder, "extras.json"), []) or []
        for row in linhas:
            username = row.get("Username")
            if not username:
                continue
            u = by_name.get(username.lower()) or {}
            add(username, u.get("UserId"), {dst: row.get(src) for src, dst in REPORT_FIELDS.items()}, "mfa")

        # via rm: precisa do dump de User completo pra resolver o Username
        # (o _id usa o Username; emitir sem ele travaria um _id errado no dia)
        rm_folder = os.path.join(folder, "rm")
        linhas = [row for rows in saved_pages(rm_folder).values() for row in rows]
        linhas += read_json(os.path.join(rm_folder, "extras.json"), []) or []
        if linhas and not users_complete:
            rm_adiadas.append(role_name)
            continue
        for row in linhas:
            member_id = row.get("MemberID")
            if not member_id or (row.get("MemberType") or "User") != "User":
                continue
            u = by_id.get(str(member_id).lower())
            add(u.get("Username") if u else None, member_id, {}, "rm")
    sys.stdout.flush()
    if rm_adiadas:
        log(f"[emit] membros via rm de {len(rm_adiadas)} role(s) ADIADOS: dump de User incompleto "
            f"(ficam no estado, saem na proxima execucao): {rm_adiadas[:10]}")

    # O doc user tem _id por dia: se fosse emitido com role_count parcial, a
    # versao correta de uma execucao posterior viraria 409 e o errado ficaria.
    # Por isso so sai quando TODAS as roles que contam (fora IMPLICIT_ROLES,
    # que nao entram no role_count) fecharam e o dump de User terminou.
    roles_completas = not (SKIP_ROLES or ROLES_FILTER or ROLES_MAX_COUNT) and all(
        m["status"] == "ok" for m in metas.values()
        if m["RoleName"] not in IMPLICIT_ROLES and m["RoleId"] not in IMPLICIT_ROLES)
    total_user = sem_role = 0
    if not (roles_completas and users_complete):
        log("[users] docs user NAO emitidos nesta execucao: ainda ha role aberta ou dump de User "
            "incompleto (role_count seria parcial)")
    else:
        for rows in users_pages.values():
            for row in rows:
                username = row.get("Username")
                if not username:
                    continue
                n = roles_por_user.get(username.lower(), 0)
                doc = {"doc_type": "user", "snapshot_date": sd, "snapshot_ms": sms,
                       "Username": username, "UserId": row.get("UserId"), "UserStatus": row.get("UserStatus"),
                       "LastLogin": parse_ms_date(row.get("LastLogin")), "LastLogin_raw": row.get("LastLogin"),
                       "role_count": n, "has_role": n > 0, "roles_complete": True}
                doc["_doc_id"] = make_doc_id("user", sd, row.get("UserId") or username)
                emit(doc)
                total_user += 1
                sem_role += n == 0
    sys.stdout.flush()

    for m in metas.values():
        emit({"doc_type": "role_summary", "snapshot_date": sd, "snapshot_ms": sms, "run_id": ctx["run_id"],
              "RoleId": m["RoleId"], "RoleName": m["RoleName"], "status": m["status"],
              "source_method": m.get("method"), "expected_count": m.get("expected_count"),
              "collected_count": m.get("collected_count"), "rolemember_count": m.get("rolemember_count"),
              "mfa_full_count": m.get("mfa_full_count"), "rm_full_count": m.get("rm_full_count"),
              "attempts": m.get("attempts"), "restarts": m.get("restarts", 0),
              "error": m.get("error"), "mfa_error": m.get("mfa_error"),
              "_doc_id": make_doc_id("summary", ctx["run_id"], m["RoleId"])})

    return total_member, total_user, sem_role, roles_completas, rm_adiadas


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

    users_pages, users_complete = collect_users(day_dir, clock)
    metas = run_roles(roles, day_dir, stats, clock)

    total_member, total_user, sem_role, roles_completas, rm_adiadas = emit_all(
        roles, metas, day_dir, ctx, users_pages, users_complete)

    por_status, por_via = {}, {}
    for m in metas.values():
        por_status[m["status"]] = por_status.get(m["status"], 0) + 1
        por_via[m.get("method")] = por_via.get(m.get("method"), 0) + 1
    esperado = sum(m.get("expected_count") or 0 for m in metas.values())
    coletado = sum(m.get("collected_count") or 0 for m in metas.values())
    abertas = sorted((m for m in metas.values() if m["status"] != "ok"), key=lambda m: m["RoleName"])

    emit({"doc_type": "run_summary", "snapshot_date": ctx["snapshot_date"], "snapshot_ms": ctx["snapshot_ms"],
          "run_id": ctx["run_id"], "roles_total": len(metas), "roles_ok": por_status.get("ok", 0),
          "roles_not_ok": len(abertas), "roles_mfa": por_via.get("mfa", 0), "roles_rm": por_via.get("rm", 0),
          "expected_count": esperado, "collected_count": coletado,
          "members_emitted": total_member, "users_emitted": total_user, "users_without_role": sem_role,
          "users_complete": users_complete, "roles_complete": roles_completas,
          "roles_rm_deferred": len(rm_adiadas), "duration_s": round(time.time() - ctx["t0"], 1),
          "_doc_id": make_doc_id("run", ctx["run_id"])})

    log(f"[fim] roles={len(metas)} status={por_status} via={por_via} esperado(FullCount)={esperado} "
        f"coletado={coletado} | member docs={total_member} | users={total_user} "
        f"(sem role={sem_role}, dump completo={users_complete})")
    for m in abertas:
        log(f"[fim] NAO FECHOU: {m['RoleName']} ({m['RoleId']}) status={m['status']} via={m.get('method')} "
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
