#!/usr/bin/env python3
"""
Worker do pipeline cyberark-apiprodesp-directory (fonte: CyberArk tenant
ApiProdesp, tenant abb4725, Redrock query — NAO confundir com o tenant
`cyberark-prodesp-directory` ja existente, que e o tenant `prodesp`
diferente, ver CLAUDE.md secao 8).

Clonado do `cyberark-detran-directory` (mesma arquitetura: dump de `Role`
+ `GetRoleMembers` por role + join em Python com dump de `User`, em vez
de JOIN pesado em SQL) — estrategia originalmente diagnosticada no
tenant Prodesp (incidente 2026-08-24, ver DEPLOY.md do prodesp-directory)
e reconfirmada por probe direto neste tenant (2026-09-14, ver abaixo),
nao so herdada sem checar.

⚠️ Historico deste tenant: em 2026-09-13 (ver memoria
`project_cyberark_apiprodesp_pipeline`) o probe inicial devolveu
`FullCount: 0` em User/Role/Event (so `Application` tinha 1 linha, a
propria app OAuth) — auth funcionava, mas SEM dado nenhum de diretorio.
Hipotese registrada na epoca: tenant recem-provisionado, sem sync ainda.
O time confirmou depois que era **permissao do usuario de servico**
(nao falta de sync). Reprobado em 2026-09-14: dado real presente.

✅ Probe manual feito NESTE tenant (ApiProdesp, abb4725) em 2026-09-14:
- `SELECT User.Username, User.ID AS UserId, User.Status AS UserStatus,
  User.LastLogin FROM User` funciona (mesmo formato de colunas do
  Detran/Prodesp — sem `Beneficiario_`, nao testado aqui mas seguindo o
  mesmo padrao "campo especifico de tenant", nao assumido presente).
- **Volume pequeno, parecido com o Detran**: `User` tem 7.016 linhas
  (`FullCount`), dump completo cabe numa pagina so com `PageSize=50000`.
  `Role` (dimensao): 70 roles no total.
- `GetRoleMembers` por role: soma de `FullCount` das 70 roles = 9.706
  pares usuario+role. Maior role ("PRODESP", 4.886 membros) levou ~14s
  isolada com `PageSize=2000`; demais roles grandes ("outsoucing - prod"
  3.253, "Reembolso" 1.399) bem menores. Nenhuma role travou em
  paginacao infinita.
- **`GetRoleMembers` deste tenant tambem IGNORA `PageNumber`/`PageSize`**
  e sempre devolve a role inteira numa unica resposta, com
  `hasMoreRows: false` mesmo pra role de 4.886 membros com
  `PageSize=1` — mesmo comportamento do tenant Detran (nao e universal
  entre tenants CyberArk, ver DEPLOY.md do Detran onde isso so foi
  descoberto depois de um incidente; aqui foi confirmado ANTES de
  escrever o worker). O loop abaixo usa `hasMoreRows` real (nao
  `len(linhas) < page_size`) desde o inicio, sem precisar do incidente.
- Nenhum membro do tipo `Role` (aninhada) visto nas roles sondadas
  (`sysadmin`, a maior "PRODESP") — so `Type: "User"`, mas o filtro por
  `Type` fica mantido por seguranca (mesmo padrao do Detran/Prodesp).

Formato do join (LEFT JOIN User -> RoleMember, achatado): UM DOCUMENTO
POR PAR (usuario, role). Usuario com N roles gera N documentos (os
campos do usuario se repetem em cada um). Usuario SEM nenhuma role gera
1 documento com role_id/role_name = null. O join key e User.ID (mais
robusto que Username, mesma decisao do Detran) — a query de users traz
tambem User.ID/User.Status.

As duas queries de origem SAO DUMPS COMPLETOS (sem filtro de tempo) —
por isso NAO se aplica a regra de janela <=1 dia (CLAUDE.md 3.6), que e
sobre queries filtradas por data na tabela Event. Aqui e sempre "SELECT
tudo", so paginado. Sem checkpoint: cada execucao e um snapshot completo
independente, carimbado com snapshot_date (data BRT do disparo). _id
determinístico (snapshot_date + user_id + role_id) -> rerun no mesmo dia
vira 409 (dedup benigno); a proxima noite gera um snapshot novo,
preservando o historico completo pra auditoria (ILM sem fase de delete).

Emite NDJSON no stdout (um doc por linha) + heartbeat final (3.2), logs
de diagnostico em stderr. Pensado pra rodar 1x/noite (schedule) via
input exec, com codec json_lines (volume grande, ver 3.1).
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

PIPELINE_NAME = "cyberark-apiprodesp-directory"


# ---------------------------------------------------------------- config ---

def _env(name, default=None, cast=str):
    val = os.environ.get(name)
    if val is None or val == "":
        return default
    return cast(val)


TOKEN_URL = _env("APIPRODESP_TOKEN_URL", "https://abb4725.id.cyberark.cloud/OAuth2/Token/ApiProdesp")
QUERY_URL = _env("APIPRODESP_QUERY_URL", "https://abb4725.id.cyberark.cloud/Redrock/query")
ROLE_MEMBERS_URL = _env("APIPRODESP_ROLE_MEMBERS_URL", "https://abb4725.id.cyberark.cloud/Roles/GetRoleMembers")

CLIENT_ID = _env("APIPRODESP_CLIENT_ID")
CLIENT_SECRET = _env("APIPRODESP_CLIENT_SECRET")

# Users: volume pequeno (7.016 linhas medido por probe 2026-09-14) -- uma
# pagina so com PageSize grande ja cobre tudo.
USERS_PAGE_SIZE = _env("APIPRODESP_USERS_PAGE_SIZE", 50000, int)
USERS_MAX_PAGES = _env("APIPRODESP_USERS_MAX_PAGES", 0, int)  # 0 = sem limite; use >0 so pra smoke test

# roles: dump da tabela Role (dimensao pequena) + GetRoleMembers por role
# -- NAO usa RoleMember via /Redrock/query (mesmo padrao Detran/Prodesp).
ROLES_PAGE_SIZE = _env("APIPRODESP_ROLES_PAGE_SIZE", 2000, int)  # paginacao dentro de UMA role (GetRoleMembers)
ROLES_MAX_WORKERS = _env("APIPRODESP_ROLES_MAX_WORKERS", 5, int)  # roles em paralelo
ROLES_MAX_COUNT = _env("APIPRODESP_ROLES_MAX_COUNT", 0, int)  # 0 = todas; use >0 so pra smoke test

# Teto duro por role herdado do incidente do Prodesp/Detran (role
# paginando sem nunca esvaziar, travava fetch_all_roles inteiro em
# silencio). Nao observado no probe deste tenant (2026-09-14: todas as
# 70 roles respondem hasMoreRows=false numa pagina so) -- mantido como
# protecao contra anomalia futura.
ROLES_MAX_PAGES_PER_ROLE = _env("APIPRODESP_ROLES_MAX_PAGES_PER_ROLE", 20, int)
# timeout por chamada ao GetRoleMembers -- probe manual 2026-09-14 (tenant
# ApiProdesp, abb4725, 70 roles no total): maior role ("PRODESP", 4.886
# membros) levou ~14s isolada com PageSize=2000. Mesma margem aplicada no
# Detran (45s sobre ~26s observado isolado) mantida aqui por consistencia
# de plataforma, mesmo com volume menor -- nao ha necessidade medida de
# encurtar, e evita repetir o erro do Prodesp (timeout raso demais cortando
# roles legitimas, ver CLAUDE.md/DEPLOY.md do prodesp-directory).
ROLE_MEMBERS_TIMEOUT_SECONDS = _env("APIPRODESP_ROLE_MEMBERS_TIMEOUT_SECONDS", 45, int)

MAX_RETRIES = _env("APIPRODESP_MAX_RETRIES", 3, int)
RETRY_BACKOFF_SECONDS = _env("APIPRODESP_RETRY_BACKOFF_SECONDS", 5, int)

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
            self.expiry = datetime.now() + timedelta(minutes=_env("APIPRODESP_TOKEN_TTL_MINUTES", 15, int))
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

            return r.json().get("Result", {}) or {}

        except requests.RequestException as e:
            last_error = str(e)
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise Exception(f"{tag} falhou apos {MAX_RETRIES} tentativas: {last_error}")


def request_query(script, page, page_size):
    """/Redrock/query — dump de tabela (Users, dimensao Role). Respeita
    PageNumber/PageSize (mesmo comportamento confirmado no Detran/Prodesp
    por probe)."""
    body = {"Script": script, "Args": {"PageNumber": page, "PageSize": page_size, "Caching": -1}}
    result = post_with_retry(QUERY_URL, body, timeout=240, tag=f"query pagina {page}")
    return [item.get("Row", {}) for item in result.get("Results", [])]


def request_role_members(role_id, page, page_size):
    """/Roles/GetRoleMembers — membros diretos de UMA role (ver nota do
    incidente no topo: substitui o JOIN via RoleMember, que nao funciona
    nesse backend).

    ⚠️ Diferente do /Redrock/query: este endpoint IGNORA PageNumber/
    PageSize e sempre devolve a role INTEIRA numa unica resposta (probe
    2026-09-14 confirmou isso ate pra role de 4.886 membros). Por isso a
    paginacao usa o campo `hasMoreRows` da resposta, e NAO
    `len(linhas) < page_size` (que nunca fecha quando a role e maior que
    `page_size` -- foi o que causou o incidente equivalente no worker do
    Detran, evitado aqui desde o inicio)."""
    body = {"Name": role_id, "Args": {"PageNumber": page, "PageSize": page_size, "Caching": -1}}
    result = post_with_retry(ROLE_MEMBERS_URL, body, timeout=ROLE_MEMBERS_TIMEOUT_SECONDS, tag=f"role {role_id} pagina {page}")
    linhas = [item.get("Row", {}) for item in result.get("Results", [])]
    return linhas, bool(result.get("hasMoreRows"))


# --------------------------------------------------------------- helpers ---

_MS_DATE_RE = re.compile(r"/Date\((\d+)\)/")


def parse_cyberark_date_ms(value):
    """Campos de data da CyberArk costumam vir em '/Date(<ms>)/'. Confirmado
    no probe (2026-09-14) que LastLogin desta fonte vem nesse formato."""
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
    """Tabela Role e pequena (dimensao, 70 linhas medido em probe neste
    tenant) e rapida via /Redrock/query normalmente (sem JOIN)."""
    roles = request_query(ROLE_DIMENSION_SCRIPT, 1, 2000)
    log(f"[roles] dimensao Role: {len(roles)} roles")
    return roles


def fetch_role_members(role_id, role_name, roles_by_key, lock):
    """GetRoleMembers de UMA role, paginado. Filtra so Type == 'User'
    (membros diretos de pessoa) — Type == 'Role' e uma role aninhada
    dentro de outra; nao expandido recursivamente por enquanto, so
    logado."""
    pagina = 1
    total_users = 0
    total_roles_aninhadas = 0

    while True:
        if pagina > ROLES_MAX_PAGES_PER_ROLE:
            # ver nota do incidente equivalente no Detran/Prodesp: role
            # anomala paginando sem nunca esvaziar -- para aqui em vez de
            # travar o worker inteiro. Os membros ja vistos ate agora desta
            # role ficam validos; so para de tentar mais paginas.
            log(f"[roles][{role_id}] ALERTA: atingiu APIPRODESP_ROLES_MAX_PAGES_PER_ROLE="
                f"{ROLES_MAX_PAGES_PER_ROLE} ({total_users} membros ja coletados) sem "
                f"esvaziar a paginacao -- parando essa role, provavel anomalia no backend")
            break

        linhas, has_more = request_role_members(role_id, pagina, ROLES_PAGE_SIZE)

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

        if not has_more:
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
        log(f"[roles] APIPRODESP_ROLES_MAX_COUNT={ROLES_MAX_COUNT} atingido (smoke test)")

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
    User.LastLogin
FROM
    User
ORDER BY
    User.Username
"""
# NOTA (probe 2026-09-14): mesma forma de query usada no Detran (sem
# User.Beneficiario_, campo especifico do tenant Prodesp -- CLAUDE.md 8:
# "campos podem existir num tenant e nao noutro"). Nao reconfirmado aqui
# se User.Beneficiario_ existiria; nao incluido por nao ter sido pedido.


def run_join(snapshot_ms, snapshot_date):
    roles_by_key = fetch_all_roles()
    matched_keys = set()

    pagina = 1
    total_users = 0
    total_docs = 0

    while True:
        if USERS_MAX_PAGES and pagina > USERS_MAX_PAGES:
            log(f"[users] APIPRODESP_USERS_MAX_PAGES={USERS_MAX_PAGES} atingido, parando (smoke test)")
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
        log("APIPRODESP_CLIENT_ID / APIPRODESP_CLIENT_SECRET nao configurados")
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
