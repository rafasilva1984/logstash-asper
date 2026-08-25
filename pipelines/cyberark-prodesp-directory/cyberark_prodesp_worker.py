#!/usr/bin/env python3
"""
Worker do pipeline cyberark-prodesp-directory (fonte: CyberArk tenant
Prodesp, Redrock query, mesmo endpoint do loginprodesp).

Reescrita dos scripts originais em novopipe/ (main.txt = orquestrador,
query_users.txt = dump da tabela User, query_roles.txt = join
RoleMember/User/Role), com uma diferenca de arquitetura em relacao a
primeira versao deste pipeline: em vez de duas data streams separadas
(uma pra users, outra pra roles), o JOIN e feito aqui no worker e o
resultado sai como UM UNICO NDJSON, pra uma UNICA data stream — pedido
explicito do cliente (Elastic nao faz JOIN em tempo de consulta entre
data streams; pra virar "uma coisa so" o join precisa acontecer antes de
indexar).

Contexto do cliente (registrado aqui pra nao se perder): o Main original
e so o script de controle que chama as duas extracoes. A Roles era a
base inicial, mas usuarios sem nenhuma role nao apareciam nela — por
isso a Users foi criada (dump completo da tabela User) pra garantir que
todo usuario aparece, tenha role ou nao.

⚠️ INCIDENTE registrado no primeiro deploy (2026-08-24), pra nao repetir
o diagnostico: a query original de roles (`query_roles.txt`, JOIN
RoleMember/User/Role via `/Redrock/query` com `split_part`/
`regexp_replace` no ON) trava o backend — confirmado por probe manual
que ATE `SELECT RoleMember.ID FROM RoleMember` CRU, sem nenhum JOIN,
com `PageSize:1`, nunca retorna (timeout, 0 bytes). Nao e problema de
volume nem de paginacao: a tabela `RoleMember` simplesmente nao e
consultavel em bulk via `/Redrock/query` nesse tenant (o script CSV
original provavelmente sempre teve esse problema de forma silenciosa —
quando uma pagina falhava nas 3 tentativas, `buscar_pagina` retornava
lista vazia, que o loop interpretava como "fim dos dados" em vez de
erro, entao os CSVs de roles gerados podem estar incompletos sem
ninguem perceber).
A alternativa que FUNCIONA (validada por probe, resposta em <1s mesmo
pra roles com dezenas de membros): o endpoint dedicado
`POST /Roles/GetRoleMembers` (REST do Centrify/CyberArk Identity, fora
do `/Redrock/query`), chamado UMA VEZ POR ROLE — a tabela `Role`
(dimensao, so ~211 linhas) e rapida via `/Redrock/query` normalmente,
entao a estrategia virou: dump de `Role` (rapido) + `GetRoleMembers`
por role (rapido, paralelizavel) + join em Python com o dump de `User`,
em vez do JOIN pesado em SQL. Ver DEPLOY.md para o probe completo que
levou a essa conclusao.
Outra particularidade do backend, medida por probe: `SELECT ... FROM
User` tem custo por linha real (~15s fixos + ~0.8ms/linha retornada),
entao `PageSize` grande (`PRODESP_USERS_PAGE_SIZE`) importa bastante —
com ~1.25M usuarios, o dump completo fica em torno de 20-25 min mesmo
com `PageSize` alto; nao ha como zerar esse custo, so amortizar.

Formato do join (LEFT JOIN User -> RoleMember, achatado): UM DOCUMENTO
POR PAR (usuario, role). Usuario com N roles gera N documentos (os
campos do usuario se repetem em cada um — e a forma mais facil de usar
no Discover/Lens do Kibana, tudo plano, sem precisar de nested queries).
Usuario SEM nenhuma role gera 1 documento com role_id/role_name = null.
O join key e User.ID (mais robusto que Username): a query de users foi
estendida pra trazer tambem User.ID/User.Status, que antes so vinham
pela query de roles.

As duas queries de origem SAO DUMPS COMPLETOS (sem filtro de tempo) —
por isso NAO se aplica a regra de janela <=1 dia (CLAUDE.md 3.6), que e
sobre queries filtradas por data na tabela Event. Aqui e sempre "SELECT
tudo", so paginado. Sem checkpoint: cada execucao e um snapshot completo
independente, carimbado com snapshot_date (data BRT do disparo). _id
determinístico (snapshot_date + user_id + role_id) -> rerun no mesmo dia
vira 409 (dedup benigno); a proxima noite gera um snapshot novo,
preservando o historico completo pra auditoria (ILM sem fase de delete).

Emite NDJSON no stdout (um doc por linha) + heartbeat final (3.2), logs
de diagnostico em stderr. Pensado pra rodar 1x/noite (20:00 BRT) via
`schedule` no input exec, com codec json_lines (volume grande, ver 3.1).
"""

import hashlib
import json
import os
import re
import sys
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

PIPELINE_NAME = "cyberark-prodesp-directory"


# ---------------------------------------------------------------- config ---

def _env(name, default=None, cast=str):
    val = os.environ.get(name)
    if val is None or val == "":
        return default
    return cast(val)


TOKEN_URL = _env("PRODESP_TOKEN_URL", "https://prodesp.id.cyberark.cloud/OAuth2/Token/PainelProdesp")
QUERY_URL = _env("PRODESP_QUERY_URL", "https://prodesp.id.cyberark.cloud/Redrock/query")
ROLE_MEMBERS_URL = _env("PRODESP_ROLE_MEMBERS_URL", "https://prodesp.id.cyberark.cloud/Roles/GetRoleMembers")

CLIENT_ID = _env("PRODESP_CLIENT_ID")
CLIENT_SECRET = _env("PRODESP_CLIENT_SECRET")

# custo por linha real nesse backend (~15s fixo + ~0.8ms/linha, medido por
# probe) -> PageSize grande amortiza o fixo; ver nota do incidente no topo.
USERS_PAGE_SIZE = _env("PRODESP_USERS_PAGE_SIZE", 50000, int)
USERS_MAX_PAGES = _env("PRODESP_USERS_MAX_PAGES", 0, int)  # 0 = sem limite; use >0 so pra smoke test

# roles: dump da tabela Role (dimensao pequena) + GetRoleMembers por role
# (ver nota do incidente) — NAO usa mais RoleMember via /Redrock/query.
ROLES_PAGE_SIZE = _env("PRODESP_ROLES_PAGE_SIZE", 2000, int)  # paginacao dentro de UMA role (GetRoleMembers)
ROLES_MAX_WORKERS = _env("PRODESP_ROLES_MAX_WORKERS", 5, int)  # roles em paralelo
ROLES_MAX_COUNT = _env("PRODESP_ROLES_MAX_COUNT", 0, int)  # 0 = todas; use >0 so pra smoke test

MAX_RETRIES = _env("PRODESP_MAX_RETRIES", 3, int)
RETRY_BACKOFF_SECONDS = _env("PRODESP_RETRY_BACKOFF_SECONDS", 5, int)

BRT_OFFSET = timedelta(hours=3)  # Brasil sem horario de verao desde 2019 -> UTC-3 fixo


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sys.stderr.write(f"[{ts}] {msg}\n")
    sys.stderr.flush()


# ------------------------------------------------------------------ auth ---

class TokenManager:
    """Thread-safe: usado tambem na coleta de roles (varios grupos em
    paralelo), o refresh acontece dentro do lock pra nao ter corrida."""

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

            j = r.json()
            self.token = j["access_token"]
            # a API nao informa expiry explicito no payload original; usa um teto
            # conservador e deixa o 401 (ver request_query) forcar refresh antes disso.
            self.expiry = datetime.now() + timedelta(minutes=_env("PRODESP_TOKEN_TTL_MINUTES", 15, int))
            return self.token

    def invalidate(self):
        with self._lock:
            self.token = None
            self.expiry = None


token_manager = TokenManager()


def auth_headers():
    return {
        "Authorization": f"Bearer {token_manager.get_token()}",
        "Content-Type": "application/json",
    }


# --------------------------------------------------------------- query ---

TRANSIENT_HINTS = ("timeout", "stream", "npgsql", "econnreset", "connection")


def post_with_retry(url, body, timeout, tag):
    """POST generico com retry: 401 renova token; erro com cara de
    transiente (stream/npgsql/timeout/conexao, ver CLAUDE.md 3.7) retenta
    com backoff; o resto e permanente e sobe na hora (aborta essa
    chamada sem consumir os retries a toa). Usado tanto pro /Redrock/query
    (Users, dimensao Role) quanto pro /Roles/GetRoleMembers."""
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(url, json=body, headers=auth_headers(), timeout=timeout)

            if r.status_code == 401:
                token_manager.invalidate()
                last_error = "401 token expirado"
                time.sleep(1)
                continue

            if r.status_code != 200:
                text = r.text[:300]
                transiente = r.status_code >= 500 or any(h in text.lower() for h in TRANSIENT_HINTS)
                if not transiente:
                    raise Exception(f"HTTP permanente {r.status_code}: {text}")
                last_error = f"HTTP {r.status_code}: {text}"
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                continue

            resultados = r.json().get("Result", {}).get("Results", [])
            return [item.get("Row", {}) for item in resultados]

        except requests.RequestException as e:
            last_error = str(e)
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise Exception(f"{tag} falhou apos {MAX_RETRIES} tentativas: {last_error}")


def request_query(script, page, page_size):
    """/Redrock/query — dump de tabela (Users, dimensao Role)."""
    body = {"Script": script, "Args": {"PageNumber": page, "PageSize": page_size, "Caching": -1}}
    return post_with_retry(QUERY_URL, body, timeout=240, tag=f"query pagina {page}")


def request_role_members(role_id, page, page_size):
    """/Roles/GetRoleMembers — membros diretos de UMA role (ver nota do
    incidente no topo: substitui o JOIN via RoleMember, que nao funciona
    nesse backend). timeout curto (30s) porque, ao contrario do
    /Redrock/query, essa chamada e rapida (~0.1-0.5s medido em probe); se
    passar muito disso e sinal de algo errado, nao de volume normal."""
    body = {"Name": role_id, "Args": {"PageNumber": page, "PageSize": page_size, "Caching": -1}}
    return post_with_retry(ROLE_MEMBERS_URL, body, timeout=30, tag=f"role {role_id} pagina {page}")


# --------------------------------------------------------------- helpers ---

_MS_DATE_RE = re.compile(r"/Date\((\d+)\)/")


def parse_cyberark_date_ms(value):
    """Campos de data da CyberArk costumam vir em '/Date(<ms>)/'. Nao
    validado por probe ainda para LastLogin desta fonte especifica —
    mantem o valor bruto ao lado (ver DEPLOY.md, conferir no smoke test)."""
    if value is None:
        return None
    m = _MS_DATE_RE.match(str(value))
    return int(m.group(1)) if m else None


def make_doc_id(*parts):
    base = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def snapshot_date_brt(now_utc):
    return (now_utc - BRT_OFFSET).strftime("%Y-%m-%d")


def join_key(user_id, username):
    """User.ID e o join key preferido (mais robusto); cai pra username
    so se o ID vier vazio (nao deveria acontecer, mas nao se assume)."""
    return f"id:{user_id}" if user_id is not None else f"username:{username}"


# --------------------------------------------------------------- output ---

def emit(doc):
    sys.stdout.write(json.dumps(doc, ensure_ascii=False, default=str) + "\n")


def emit_heartbeat():
    sys.stdout.write(json.dumps({"heartbeat": PIPELINE_NAME}) + "\n")
    sys.stdout.flush()


# ----------------------------------------------------------------- roles ---

ROLE_DIMENSION_SCRIPT = """
SELECT
    Role.ID,
    Role.Name
FROM
    Role
"""


def fetch_roles_dimension():
    """Tabela Role e pequena (dimensao, ~211 linhas medido em probe) e
    rapida via /Redrock/query normalmente (sem JOIN) — so essa parte
    ainda usa /Redrock/query pro lado de roles."""
    roles = request_query(ROLE_DIMENSION_SCRIPT, 1, 2000)
    log(f"[roles] dimensao Role: {len(roles)} roles")
    return roles


def fetch_role_members(role_id, role_name, roles_by_key, lock):
    """GetRoleMembers de UMA role, paginado. Filtra so Type == 'User'
    (membros diretos de pessoa) — Type == 'Role' e uma role aninhada
    dentro de outra (ex.: 'Acesso API' dentro de 'sysadmin', visto em
    probe); nao expandido recursivamente por enquanto, so logado."""
    pagina = 1
    total_users = 0
    total_roles_aninhadas = 0

    while True:
        linhas = request_role_members(role_id, pagina, ROLES_PAGE_SIZE)

        if not linhas:
            break

        with lock:
            for row in linhas:
                if row.get("Type") != "User":
                    total_roles_aninhadas += 1
                    continue
                key = join_key(row.get("Guid"), row.get("Name"))
                entry = {"role_id": role_id, "role_name": role_name}
                lst = roles_by_key.setdefault(key, [])
                if entry not in lst:
                    lst.append(entry)
                total_users += 1

        if len(linhas) < ROLES_PAGE_SIZE:
            break

        pagina += 1

    if total_roles_aninhadas:
        log(f"[roles][{role_id}] {total_roles_aninhadas} membro(s) do tipo Role (aninhada), ignorados")

    return total_users


def fetch_all_roles():
    """Coleta TODAS as roles antes de comecar a emitir (precisa das duas
    pontas do join em memoria). Volume tipico e roles << users (cada
    usuario tem poucas roles), entao manter em dict e leve."""
    roles_dim = fetch_roles_dimension()
    if ROLES_MAX_COUNT:
        roles_dim = roles_dim[:ROLES_MAX_COUNT]
        log(f"[roles] PRODESP_ROLES_MAX_COUNT={ROLES_MAX_COUNT} atingido (smoke test)")

    roles_by_key = {}
    lock = threading.Lock()
    total_membros = 0

    with ThreadPoolExecutor(max_workers=ROLES_MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_role_members, r.get("ID"), r.get("Name"), roles_by_key, lock): r.get("ID")
            for r in roles_dim
        }
        for future in as_completed(futures):
            role_id = futures[future]
            try:
                total_membros += future.result()
            except Exception as e:
                # role isolada falhando nao deve derrubar as outras; os
                # usuarios dessa role so aparecem sem ela neste snapshot
                # (ficam corretos de novo no snapshot da proxima noite).
                log(f"[roles][{role_id}] falhou: {e}")

    log(f"[roles] coleta finalizada: roles={len(roles_dim)} membros={total_membros} "
        f"usuarios_distintos_com_role={len(roles_by_key)}")
    return roles_by_key


# ------------------------------------------------------------------ join ---

USERS_SCRIPT = """
SELECT
    User.Username,
    User.ID AS UserId,
    User.Status AS UserStatus,
    User.LastLogin,
    User.Beneficiario_
FROM
    User
ORDER BY
    User.Username
"""


def run_join(snapshot_ms, snapshot_date):
    roles_by_key = fetch_all_roles()
    matched_keys = set()

    pagina = 1
    total_users = 0
    total_docs = 0

    while True:
        if USERS_MAX_PAGES and pagina > USERS_MAX_PAGES:
            log(f"[users] PRODESP_USERS_MAX_PAGES={USERS_MAX_PAGES} atingido, parando (smoke test)")
            break

        log(f"[users] pagina {pagina}")
        linhas = request_query(USERS_SCRIPT, pagina, USERS_PAGE_SIZE)

        if not linhas:
            log("[users] pagina vazia, fim da paginacao")
            break

        for row in linhas:
            total_users += 1
            username = row.get("Username")
            user_id = row.get("UserId")
            key = join_key(user_id, username)
            matched_keys.add(key)

            base = {
                "snapshot_date": snapshot_date,
                "snapshot_ms": snapshot_ms,
                "username": username,
                "user_id": user_id,
                "user_status": row.get("UserStatus"),
                "last_login_raw": row.get("LastLogin"),
                "last_login_ms": parse_cyberark_date_ms(row.get("LastLogin")),
                "beneficiario": row.get("Beneficiario_"),
            }

            roles = roles_by_key.get(key)
            if not roles:
                doc = dict(base, role_id=None, role_name=None)
                doc["_doc_id"] = make_doc_id(snapshot_date, key, None)
                emit(doc)
                total_docs += 1
            else:
                for r in roles:
                    doc = dict(base, role_id=r["role_id"], role_name=r["role_name"])
                    doc["_doc_id"] = make_doc_id(snapshot_date, key, r["role_id"])
                    emit(doc)
                    total_docs += 1

        log(f"[users] total acumulado: {total_users} usuarios / {total_docs} documentos")

        if len(linhas) < USERS_PAGE_SIZE:
            log("[users] ultima pagina detectada")
            break

        pagina += 1
        time.sleep(0.3)

    orfaos = set(roles_by_key) - matched_keys
    if orfaos:
        log(f"[join] aviso: {len(orfaos)} usuario(s) com role mas ausentes no dump de User "
            f"(ex.: {list(orfaos)[:5]}) — roles orfas deste snapshot, nao emitidas")

    log(f"[join] finalizado: usuarios={total_users} documentos={total_docs}")


# ------------------------------------------------------------------ main --

def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        log("PRODESP_CLIENT_ID / PRODESP_CLIENT_SECRET nao configurados")
        emit_heartbeat()
        return

    now_utc = datetime.now(timezone.utc)
    snapshot_ms = int(now_utc.timestamp() * 1000)
    snapshot_date = snapshot_date_brt(now_utc)

    try:
        run_join(snapshot_ms, snapshot_date)
    except Exception as e:
        log(f"falha nao recuperada: {e}")

    emit_heartbeat()
    sys.stdout.flush()


if __name__ == "__main__":
    main()
