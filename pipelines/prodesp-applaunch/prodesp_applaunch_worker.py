#!/usr/bin/env python3
"""
Worker do pipeline prodesp-applaunch-realtime (fonte: CyberArk tenant
Prodesp, Redrock query — mesmo tenant/credencial do loginprodesp e do
cyberark-prodesp-directory, mas OUTRO evento e outro data stream).

Porta o script do cliente que exportava CSV ("usuarios_ultimas_12h_parte")
com os acessos a aplicacoes (EventType Cloud.Saas.Application.AppLaunch,
ApplicationType Web/OpenIDConnect/OAuth — SOU SP, Plataforma SP, OIDC ...)
enriquecidos com os dados do usuario (DisplayName, Status, UserType, ID,
Selo_, LastLogin). O loginprodesp ja existente so tem o evento de login
(tabela Event, sem dados de usuario) — por isso este pipeline e separado.

⚠️ DIFERENCA DELIBERADA em relacao ao script original — o JOIN: a query
original faz `User INNER JOIN Event ON User.LoginName = Event.NormalizedUser`.
Probe de 2026-10-21: esse JOIN estoura timeout (180s, 0 bytes) MESMO numa
janela de 10 minutos (ja registrado no CLAUDE.md §8 para o loginprodesp).
A mesma query so na tabela Event responde em <1s (382 eventos/10min).
Entao o join e feito aqui: (1) eventos da janela so na Event, (2) lookup
dos usuarios na User, (3) merge em Python. O lookup tem duas etapas,
tambem medidas por probe:
  a. prefetch `User.LastLogin > (inicio - USER_PREFETCH_LOOKBACK)`: 0.5-1.3s
     e cobre ~95% dos logins da janela (quem abre app logou ha pouco);
  b. o resto via `User.LoginName IN (...)`: custo FIXO de ~30s por query
     (50 ou 500 logins, mesmo tempo) e a query tem LIMITE DE 10.000
     CARACTERES ("Query string exceeds maximum length") -> lotes montados
     por tamanho da string, nao por quantidade.
Diferenca semantica: o INNER JOIN descartava evento sem usuario na tabela
User; aqui o evento e mantido com user_found=false (mais dado, nao menos).

Outras particularidades medidas no probe:
  - Event.Uuid vem NULL em todos os AppLaunch -> nao serve para _id. O _id
    e sha256 de WhenOccurred(ms)|login|IPs|aplicacao|tipo|evento (|Uuid,
    se um dia vier preenchido) — 382/382 distintos no probe (CLAUDE.md 3.8).
  - WhenLogged == WhenOccurred em 100% da amostra; o filtro e o
    @timestamp usam WhenOccurred (mesmo campo do WHERE original).
  - Latitude/Longitude do IPLookup vem como string -> convertidas para
    float + campo geo_point Location (mapa no Grafana/Kibana).
  - Selo_ vem como texto "{level=gold, reliabilities=[{id=101, ...}]}" ->
    mantido bruto + Selo_level e Selo_reliabilities extraidos.

Janela (CLAUDE.md §5, varredura unificada backfill == realtime):
  sem checkpoint -> inicio = agora - INITIAL_LOOKBACK_HOURS;
  com checkpoint -> inicio = checkpoint - OVERLAP_MINUTES.
Fatiada em pedacos de SLICE_MINUTES (<= 1 dia, 3.6), no maximo
MAX_SLICES_PER_RUN por execucao (o resto fica pra proxima). Rodando a cada
5 min com overlap de 5 min, a janela efetiva e ~10 min (proposta do
cliente). Falha de uma fatia (apos retries, 3.7) nao avanca o checkpoint.

Emite NDJSON no stdout + heartbeat final (codec json_lines, 3.1/3.2) —
realtime e pouco volume, mas catch-up/backfill pode ser grande. Logs em
stderr.

Modo manual (reprocesso), sem ler nem gravar checkpoint:
  --since 2026-10-20T00:00:00-03:00 [--until 2026-10-21T00:00:00-03:00]
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

PIPELINE_NAME = "prodesp-applaunch-realtime"


# ---------------------------------------------------------------- config ---

def _env(name, default=None, cast=str):
    val = os.environ.get(name)
    if val is None or val == "":
        return default
    return cast(val)


# Prefixo PRODESP_APPLAUNCH_ em tudo: os .env de todos os pipelines entram
# no MESMO processo do Logstash (EnvironmentFile da unit) — PRODESP_* ja e
# do cyberark-prodesp-directory e do loginprodesp.
TOKEN_URL = _env("PRODESP_APPLAUNCH_TOKEN_URL", "https://prodesp.id.cyberark.cloud/OAuth2/Token/PainelProdesp")
QUERY_URL = _env("PRODESP_APPLAUNCH_QUERY_URL", "https://prodesp.id.cyberark.cloud/Redrock/query")
CLIENT_ID = _env("PRODESP_APPLAUNCH_CLIENT_ID")
CLIENT_SECRET = _env("PRODESP_APPLAUNCH_CLIENT_SECRET")

PAGE_SIZE = _env("PRODESP_APPLAUNCH_PAGE_SIZE", 1000, int)
USER_PREFETCH_LOOKBACK_MINUTES = _env("PRODESP_APPLAUNCH_USER_PREFETCH_LOOKBACK_MINUTES", 120, int)
USER_PREFETCH_MAX_HOURS = _env("PRODESP_APPLAUNCH_USER_PREFETCH_MAX_HOURS", 6, int)  # catch-up antigo -> so IN
USER_PREFETCH_PAGE_SIZE = _env("PRODESP_APPLAUNCH_USER_PREFETCH_PAGE_SIZE", 10000, int)
MAX_QUERY_CHARS = 9500  # backend recusa Script > 10.000 caracteres
QUERY_TIMEOUT = _env("PRODESP_APPLAUNCH_QUERY_TIMEOUT_SECONDS", 180, int)
MAX_RETRIES = _env("PRODESP_APPLAUNCH_MAX_RETRIES", 3, int)
RETRY_BACKOFF = _env("PRODESP_APPLAUNCH_RETRY_BACKOFF_SECONDS", 5, int)

CHECKPOINT_FILE = _env("PRODESP_APPLAUNCH_CHECKPOINT_FILE", "/var/lib/logstash/prodesp-applaunch/checkpoint")
INITIAL_LOOKBACK_HOURS = _env("PRODESP_APPLAUNCH_INITIAL_LOOKBACK_HOURS", 1, int)
OVERLAP_MINUTES = _env("PRODESP_APPLAUNCH_OVERLAP_MINUTES", 5, int)
SLICE_MINUTES = min(_env("PRODESP_APPLAUNCH_SLICE_MINUTES", 60, int), 1440)  # teto 1 dia (3.6)
MAX_SLICES_PER_RUN = _env("PRODESP_APPLAUNCH_MAX_SLICES_PER_RUN", 6, int)

TRANSIENT_HINTS = ("timeout", "timed out", "stream", "npgsql", "deadlock", "econnreset", "connection")


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sys.stderr.write(f"[{ts}] {msg}\n")
    sys.stderr.flush()


def to_cyberark_date(dt):
    """UTC, sem sufixo de timezone (CLAUDE.md 3.5)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------------------------------------------ auth ---

class TokenManager:
    def __init__(self):
        self.token = None
        self.expiry = 0

    def get(self):
        if self.token and time.monotonic() < self.expiry:
            return self.token
        r = requests.post(TOKEN_URL, data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
            "scope": "all",
        }, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Erro token: {r.status_code} - {r.text[:200]}")
        j = r.json()
        self.token = j["access_token"]
        ttl = float(j.get("expires_in") or 900)
        self.expiry = time.monotonic() + max(60, ttl - 60)
        return self.token

    def invalidate(self):
        self.token = None


token_manager = TokenManager()


class PermanentError(Exception):
    pass


def query(script, page, page_size):
    """POST /Redrock/query com retry (3.7): 401 renova token; erro com cara
    de transiente retenta com backoff; o resto e permanente e sobe na hora."""
    body = {"Script": script, "Args": {"PageNumber": page, "PageSize": page_size, "Caching": -1}}
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(QUERY_URL, json=body, timeout=QUERY_TIMEOUT, headers={
                "Authorization": f"Bearer {token_manager.get()}",
                "Content-Type": "application/json",
            })
        except requests.RequestException as e:
            last_error = f"{type(e).__name__}: {e}"
            log(f"  tentativa {attempt}/{MAX_RETRIES}: {last_error}")
            time.sleep(RETRY_BACKOFF * attempt)
            continue

        if r.status_code == 401:
            token_manager.invalidate()
            last_error = "401 token expirado"
            continue
        if r.status_code != 200:
            text = r.text[:300]
            if r.status_code < 500 and not any(h in text.lower() for h in TRANSIENT_HINTS):
                raise PermanentError(f"HTTP {r.status_code}: {text}")
            last_error = f"HTTP {r.status_code}: {text}"
            log(f"  tentativa {attempt}/{MAX_RETRIES}: {last_error}")
            time.sleep(RETRY_BACKOFF * attempt)
            continue

        j = r.json()
        if not j.get("success"):
            msg = str(j.get("Message") or "erro desconhecido na query")
            if not any(h in msg.lower() for h in TRANSIENT_HINTS):
                raise PermanentError(f"query recusada: {msg[:300]}")
            last_error = msg[:300]
            log(f"  tentativa {attempt}/{MAX_RETRIES}: {last_error}")
            time.sleep(RETRY_BACKOFF * attempt)
            continue

        return [item.get("Row", {}) for item in (j.get("Result") or {}).get("Results", [])]

    raise RuntimeError(f"pagina {page} falhou apos {MAX_RETRIES} tentativas: {last_error}")


# --------------------------------------------------------------- queries ---

# Mesmos filtros e colunas de Event da query original; colunas de User vem
# do lookup (ver docstring: o JOIN em SQL estoura timeout nesse tenant).
EVENTS_SCRIPT = """
SELECT
    Event.NormalizedUser AS LoginName,
    Event.ClientIPAddress,
    Event.FromIPAddress,
    IPLookup(Event.FromIPAddress,'country_name') AS Country,
    IPLookup(Event.FromIPAddress,'city') AS City,
    IPLookup(Event.FromIPAddress,'longitude') AS Longitude,
    IPLookup(Event.FromIPAddress,'latitude') AS Latitude,
    Event.ApplicationName,
    Event.ApplicationType,
    Event.AppName,
    Event.EventType,
    Event.WhenLogged,
    Event.WhenOccurred,
    Event.Uuid
FROM Event
WHERE Event.EventType = "Cloud.Saas.Application.AppLaunch"
AND Event.ApplicationType IN ("Web","OpenIDConnect","OAuth")
AND Event.WhenOccurred > Datefunc('{inicio}')
AND Event.WhenOccurred <= Datefunc('{fim}')
ORDER BY Event.WhenOccurred ASC
"""

USERS_SELECT = """
SELECT
    User.LoginName,
    User.DisplayName,
    User.Status,
    User.UserType,
    User.ID,
    User.Selo_,
    User.LastLogin
FROM User
"""
USERS_BY_LASTLOGIN_SCRIPT = USERS_SELECT + "WHERE User.LastLogin > Datefunc('{desde}')\n"
USERS_BY_LOGIN_SCRIPT = USERS_SELECT + "WHERE User.LoginName IN ({logins})\n"


def fetch_events(inicio, fim):
    script = EVENTS_SCRIPT.format(inicio=to_cyberark_date(inicio), fim=to_cyberark_date(fim))
    rows = []
    page = 1
    while True:
        batch = query(script, page, PAGE_SIZE)
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
        time.sleep(0.2)
    log(f"  eventos: {len(rows)} ({page} pagina(s))")
    return rows


def _sql_str(value):
    return "'" + str(value).replace("'", "''") + "'"


def prefetch_users(desde, cache):
    """Etapa (a) do lookup: usuarios com login recente, em 1 query rapida
    (ver docstring). Cache vale so durante a execucao."""
    script = USERS_BY_LASTLOGIN_SCRIPT.format(desde=to_cyberark_date(desde))
    page = 1
    total = 0
    while True:
        rows = query(script, page, USER_PREFETCH_PAGE_SIZE)
        for r in rows:
            if r.get("LoginName"):
                cache[r["LoginName"]] = r
        total += len(rows)
        if len(rows) < USER_PREFETCH_PAGE_SIZE:
            break
        page += 1
    log(f"  usuarios: prefetch LastLogin > {desde.isoformat()} -> {total}")


def login_batches(logins):
    """Etapa (b): lotes de IN (...) que cabem no limite de caracteres."""
    base_len = len(USERS_BY_LOGIN_SCRIPT.format(logins=""))
    batch, size = [], base_len
    for login in logins:
        item = _sql_str(login)
        if batch and size + len(item) + 1 > MAX_QUERY_CHARS:
            yield batch
            batch, size = [], base_len
        batch.append(item)
        size += len(item) + 1
    if batch:
        yield batch


def fetch_users(logins, cache):
    """Preenche cache[login] com a linha da User (ou None se nao existe)
    para os logins que o prefetch nao cobriu."""
    pending = sorted(l for l in logins if l and l not in cache)
    for batch in login_batches(pending):
        rows = query(USERS_BY_LOGIN_SCRIPT.format(logins=",".join(batch)), 1, len(batch) * 2)
        for r in rows:
            if r.get("LoginName"):
                cache[r["LoginName"]] = r
    for login in pending:
        cache.setdefault(login, None)
    if pending:
        log(f"  usuarios: {len(pending)} logins fora do prefetch consultados via IN")


# --------------------------------------------------------------- helpers ---

_MS_DATE_RE = re.compile(r"/Date\((-?\d+)\)/")
_SELO_LEVEL_RE = re.compile(r"level=([^,}\s]+)")
_SELO_RELIABILITY_RE = re.compile(r"\{id=(\d+),\s*updatedAt=([^}]+)\}")


def date_ms(value):
    """/Date(ms)/ -> int ms (UTC)."""
    if value is None:
        return None
    m = _MS_DATE_RE.match(str(value))
    return int(m.group(1)) if m else None


def ms_to_iso(ms):
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def to_float(value):
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def parse_selo(raw):
    if not raw:
        return None, None
    m = _SELO_LEVEL_RE.search(raw)
    reliabilities = [{"id": rid, "updatedAt": upd.strip()} for rid, upd in _SELO_RELIABILITY_RE.findall(raw)]
    return (m.group(1) if m else None), (reliabilities or None)


def make_doc_id(ev, occurred_ms):
    parts = [occurred_ms, ev.get("LoginName"), ev.get("FromIPAddress"), ev.get("ClientIPAddress"),
             ev.get("ApplicationName"), ev.get("AppName"), ev.get("ApplicationType"),
             ev.get("EventType"), ev.get("Uuid")]
    base = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def build_doc(ev, user):
    occurred_ms = date_ms(ev.get("WhenOccurred"))
    lat, lon = to_float(ev.get("Latitude")), to_float(ev.get("Longitude"))
    selo_raw = user.get("Selo_") if user else None
    selo_level, selo_reliabilities = parse_selo(selo_raw)
    return {
        "_doc_id": make_doc_id(ev, occurred_ms),
        "LoginName": ev.get("LoginName"),
        "DisplayName": user.get("DisplayName") if user else None,
        "Status": user.get("Status") if user else None,
        "UserType": user.get("UserType") if user else None,
        "UserID": user.get("ID") if user else None,
        "Selo_": selo_raw,
        "Selo_level": selo_level,
        "Selo_reliabilities": selo_reliabilities,
        "LastLogin": ms_to_iso(date_ms(user.get("LastLogin"))) if user else None,
        "user_found": user is not None,
        "ClientIPAddress": ev.get("ClientIPAddress"),
        "FromIPAddress": ev.get("FromIPAddress"),
        "Country": ev.get("Country"),
        "City": ev.get("City"),
        "Latitude": lat,
        "Longitude": lon,
        "Location": {"lat": lat, "lon": lon} if lat is not None and lon is not None else None,
        "ApplicationName": ev.get("ApplicationName"),
        "ApplicationType": ev.get("ApplicationType"),
        "AppName": ev.get("AppName"),
        "EventType": ev.get("EventType"),
        "WhenLogged": ms_to_iso(date_ms(ev.get("WhenLogged"))),
        "WhenOccurred": ms_to_iso(occurred_ms),
        "Uuid": ev.get("Uuid"),
    }, occurred_ms


# --------------------------------------------------------------- output ---

def emit(doc):
    sys.stdout.write(json.dumps(doc, ensure_ascii=False, default=str) + "\n")


def emit_heartbeat():
    sys.stdout.write(json.dumps({"heartbeat": PIPELINE_NAME}) + "\n")
    sys.stdout.flush()


# ----------------------------------------------------------- checkpoint ---

def read_checkpoint():
    try:
        with open(CHECKPOINT_FILE) as f:
            iso = f.read().strip()
        return datetime.fromisoformat(iso).astimezone(timezone.utc) if iso else None
    except FileNotFoundError:
        return None
    except Exception as e:
        log(f"checkpoint ilegivel ({e}), tratando como inexistente")
        return None


def write_checkpoint(dt):
    """Atomico (.tmp + rename) e monotonico: nunca retrocede."""
    current = read_checkpoint()
    if current is not None and dt <= current:
        return
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as f:
        f.write(dt.isoformat())
    os.replace(tmp, CHECKPOINT_FILE)


# --------------------------------------------------------------- janela ---

def build_slices(now, since=None, until=None):
    if since is not None:
        start, end = since, (until or now)
    else:
        end = now
        checkpoint = read_checkpoint()
        if checkpoint is None:
            start = now - timedelta(hours=INITIAL_LOOKBACK_HOURS)
            log(f"sem checkpoint: varredura inicial das ultimas {INITIAL_LOOKBACK_HOURS}h")
        else:
            start = checkpoint - timedelta(minutes=OVERLAP_MINUTES)

    slices = []
    cursor = start
    while cursor < end:
        stop = min(cursor + timedelta(minutes=SLICE_MINUTES), end)
        slices.append((cursor, stop))
        cursor = stop  # filtro e "> inicio AND <= fim": fatias contiguas sem buraco
    return slices


def run_slice(inicio, fim, user_cache):
    """Retorna (docs_emitidos, max_occurred) — so emite depois de eventos e
    usuarios coletados por inteiro: falha no meio nao deixa doc pela metade."""
    log(f"janela {inicio.isoformat()} -> {fim.isoformat()}")
    events = fetch_events(inicio, fim)
    fetch_users({e.get("LoginName") for e in events}, user_cache)

    max_ms = None
    sem_usuario = 0
    for ev in events:
        user = user_cache.get(ev.get("LoginName"))
        doc, occurred_ms = build_doc(ev, user)
        if user is None:
            sem_usuario += 1
        if occurred_ms is not None and (max_ms is None or occurred_ms > max_ms):
            max_ms = occurred_ms
        emit(doc)
    if sem_usuario:
        log(f"  {sem_usuario} evento(s) sem usuario na tabela User (user_found=false)")
    max_dt = datetime.fromtimestamp(max_ms / 1000, tz=timezone.utc) if max_ms is not None else None
    return len(events), max_dt


# ------------------------------------------------------------------ main --

def parse_ts(value):
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise argparse.ArgumentTypeError(f"informe o fuso no timestamp: {value} (ex.: -03:00)")
    return dt.astimezone(timezone.utc)


def main():
    parser = argparse.ArgumentParser(description="Worker prodesp-applaunch-realtime (CyberArk -> NDJSON)")
    parser.add_argument("--since", type=parse_ts, help="modo manual: inicio da janela (ISO com fuso); nao usa checkpoint")
    parser.add_argument("--until", type=parse_ts, help="modo manual: fim da janela (default: agora)")
    args = parser.parse_args()
    if args.until is not None and args.since is None:
        parser.error("--until exige --since")
    manual = args.since is not None

    if not CLIENT_ID or not CLIENT_SECRET:
        log("PRODESP_APPLAUNCH_CLIENT_ID / PRODESP_APPLAUNCH_CLIENT_SECRET nao configurados")
        emit_heartbeat()
        return

    started = time.monotonic()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    slices = build_slices(now, args.since, args.until)
    if not manual and len(slices) > MAX_SLICES_PER_RUN:
        log(f"{len(slices)} fatias pendentes; processando {MAX_SLICES_PER_RUN} nesta execucao (catch-up)")
        slices = slices[:MAX_SLICES_PER_RUN]
    log(f"inicio | modo={'manual' if manual else 'checkpoint'} | fatias={len(slices)}")

    total = 0
    user_cache = {}
    if slices:
        # catch-up muito antigo traria a User inteira no prefetch -> limita;
        # quem ficar de fora sai pelo IN de cada fatia.
        desde = max(slices[0][0] - timedelta(minutes=USER_PREFETCH_LOOKBACK_MINUTES),
                    now - timedelta(hours=USER_PREFETCH_MAX_HOURS))
        try:
            prefetch_users(desde, user_cache)
        except Exception as e:
            log(f"  prefetch de usuarios falhou ({e}); seguindo so com IN")
    for inicio, fim in slices:
        try:
            docs, max_dt = run_slice(inicio, fim, user_cache)
        except Exception as e:
            log(f"falha na janela {inicio.isoformat()} -> {fim.isoformat()}: {e} "
                f"— checkpoint NAO avanca, retoma na proxima execucao")
            break
        total += docs
        sys.stdout.flush()
        if not manual:
            # max visto (ou fim da fatia se vazia), CLAUDE.md §5
            write_checkpoint(max_dt or fim)
    else:
        if slices and slices[-1][1] >= now:
            log("Alcancado o presente")

    log(f"fim | docs={total} | tempo={time.monotonic() - started:.1f}s")
    emit_heartbeat()


if __name__ == "__main__":
    main()
