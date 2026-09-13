#!/usr/bin/env python3
"""
Worker do pipeline sws-online-users (fonte: API Alero).

Snapshot de "quem esta online agora": varre sessions das ultimas
SWS_ONLINE_LOOKBACK_HOURS (default 13h, em chunks de 1h) e, para cada uma,
checa se teve step nos ultimos SWS_ONLINE_ACTIVITY_MINUTES (default 15min)
via /sws/sessions/{id}/steps. O lookback largo (13h) e o que garante achar
sessao longa ainda aberta que comecou muito antes da janela de atividade —
mesmo problema descrito no CLAUDE.md 3.12, resolvido aqui dentro de UMA
unica execucao (nao precisa de pipeline de reconciliacao separado).

Emite NDJSON no stdout (uma linha por documento). Logs de diagnostico vao
para stderr — nunca para stdout (input exec + codec plain no .conf).

Cada execucao compara o conjunto de usuarios ativos agora contra o estado
persistido da execucao anterior (SWS_ONLINE_STATE_FILE) e emite, por
usuario:
  - status=ENTROU   : ficou ativo agora, nao estava na execucao anterior
  - status=ATIVO     : continua ativo (estava e continua)
  - status=SAIU      : estava ativo na execucao anterior, nao esta mais

Sem heartbeat: saida pequena (poucas dezenas de docs por execucao), usa
codec=>plain no .conf (ver CLAUDE.md 3.1/4.1) — heartbeat so e necessario
para json_lines.
"""

import hashlib
import json
import os
import sys
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests
import urllib3

urllib3.disable_warnings()

PIPELINE_NAME = "sws-online-users"


# ---------------------------------------------------------------- config ---

def _env(name, default=None, cast=str):
    val = os.environ.get(name)
    if val is None or val == "":
        return default
    return cast(val)


AUTH_URL = _env("ALERO_AUTH_URL", "https://auth.alero.io/auth/realms/serviceaccounts/protocol/openid-connect/token")
CLIENT_ID = _env("ALERO_CLIENT_ID")
CLIENT_SECRET = _env("ALERO_CLIENT_SECRET")
BASE_URL = _env("ALERO_BASE_URL", "https://api.alero.io/v2-edge")

LOOKBACK_HOURS = _env("SWS_ONLINE_LOOKBACK_HOURS", 13, int)
CHUNK_HOURS = _env("SWS_ONLINE_CHUNK_HOURS", 1, int)
ACTIVITY_MINUTES = _env("SWS_ONLINE_ACTIVITY_MINUTES", 15, int)
PAGE_LIMIT = _env("SWS_ONLINE_PAGE_LIMIT", 100, int)
MAX_WORKERS = _env("SWS_ONLINE_MAX_WORKERS", 50, int)
MAX_RETRIES = _env("SWS_ONLINE_MAX_RETRIES", 3, int)

STATE_FILE = _env("SWS_ONLINE_STATE_FILE", "/var/lib/logstash/sws-online-users/state.json")


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sys.stderr.write(f"[{ts}] {msg}\n")
    sys.stderr.flush()


# ------------------------------------------------------------------ auth ---

class TokenManager:
    """Thread-safe: refresh acontece dentro do lock pra nao ter corrida
    quando varios workers batem em get_token() ao mesmo tempo."""

    def __init__(self):
        self.token = None
        self.expiry = None
        self._lock = threading.Lock()

    def get_token(self):
        with self._lock:
            if self.token and self.expiry and datetime.now() < self.expiry:
                return self.token

            payload = {
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type": "client_credentials",
                "scope": "openid",
            }
            r = requests.post(AUTH_URL, data=payload, verify=False, timeout=30)
            if r.status_code != 200:
                raise Exception(f"Erro ao obter token: {r.status_code} - {r.text[:200]}")

            j = r.json()
            self.token = j["access_token"]
            self.expiry = datetime.now() + timedelta(seconds=j["expires_in"] - 30)
            return self.token

    def invalidate(self):
        with self._lock:
            self.token = None
            self.expiry = None


token_manager = TokenManager()


def auth_headers():
    return {"Authorization": f"Bearer {token_manager.get_token()}"}


def create_session_with_pool():
    session = requests.Session()
    from requests.adapters import HTTPAdapter
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def request_retry(http, url, params=None, max_attempts=None):
    """Retry robusto: 429 (Retry-After), 401 (refresh de token), 5xx e erros
    de rede com backoff exponencial (teto 30s) -> transiente, retenta. 4xx
    nao recuperavel propaga na hora -> falha permanente."""
    max_attempts = max_attempts or MAX_RETRIES
    last_error = None

    for attempt in range(max_attempts):
        try:
            r = http.get(url, headers=auth_headers(), params=params, verify=False, timeout=30)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 401:
                token_manager.invalidate()
                last_error = f"401 nao autorizado: {r.text[:200]}"
                time.sleep(1)
                continue

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = int(ra) if ra and ra.isdigit() else min(2 ** attempt, 30)
                last_error = f"429 throttle, aguardando {wait}s"
                time.sleep(wait)
                continue

            if r.status_code >= 500:
                last_error = f"Erro servidor {r.status_code}: {r.text[:200]}"
                time.sleep(min(2 ** attempt, 30))
                continue

            raise Exception(f"Erro permanente {r.status_code}: {r.text[:200]}")

        except requests.RequestException as e:
            last_error = str(e)
            time.sleep(min(2 ** attempt, 30))

    raise Exception(f"Erro apos {max_attempts} tentativas: {last_error}")


# ----------------------------------------------------------- coleta API ---

def get_sessions(http, agora):
    """Varre as ultimas LOOKBACK_HOURS em chunks de CHUNK_HOURS, paginando
    cada chunk por offset ate a pagina vir menor que PAGE_LIMIT."""
    sessions_all = []
    url = f"{BASE_URL}/sws/sessions"

    for i in range(0, LOOKBACK_HOURS, CHUNK_HOURS):
        tempo_fim = agora - timedelta(hours=i)
        tempo_inicio = agora - timedelta(hours=i + CHUNK_HOURS)
        from_ms = int(tempo_inicio.timestamp() * 1000)
        to_ms = int(tempo_fim.timestamp() * 1000)

        offset = 0
        chunk_sessions = []
        while True:
            params = {"fromTime": from_ms, "toTime": to_ms, "offset": offset, "limit": PAGE_LIMIT}
            data = request_retry(http, url, params=params)
            batch = data.get("sessions", [])
            if not batch:
                break
            chunk_sessions.extend(batch)
            if len(batch) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT

        sessions_all.extend(chunk_sessions)
        log(f"  chunk {i}-{i + CHUNK_HOURS}h: {len(chunk_sessions)} sessions")

    log(f"total sessions no lookback ({LOOKBACK_HOURS}h): {len(sessions_all)}")
    return sessions_all


def session_has_recent_steps(http, session_id, agora):
    tempo_min = agora - timedelta(minutes=ACTIVITY_MINUTES)
    url = f"{BASE_URL}/sws/sessions/{session_id}/steps"
    params = {
        "fromTime": int(tempo_min.timestamp() * 1000),
        "toTime": int(agora.timestamp() * 1000),
        "limit": 1,
    }
    try:
        data = request_retry(http, url, params=params, max_attempts=1)
    except Exception:
        return False
    return len(data.get("sessionSteps", [])) > 0


# --------------------------------------------------------------- output ---

def emit(doc):
    sys.stdout.write(json.dumps(doc, ensure_ascii=False, default=str) + "\n")


def make_doc_id(*parts):
    base = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


# ------------------------------------------------------------------ state ---

def read_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        log(f"estado ilegivel ({e}), tratando como vazio")
        return {}


def write_state(usuarios_agora):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(usuarios_agora, f, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


# ------------------------------------------------------------------ main --

def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        log("ALERO_CLIENT_ID / ALERO_CLIENT_SECRET nao configurados")
        return

    agora = datetime.now(timezone.utc)
    run_time_ms = int(agora.timestamp() * 1000)

    log(f"inicio run {agora.isoformat()} (lookback={LOOKBACK_HOURS}h, atividade={ACTIVITY_MINUTES}min)")

    http = create_session_with_pool()
    try:
        sessions_all = get_sessions(http, agora)
        if not sessions_all:
            log("nenhuma session no lookback")
            usuarios_agora = {}
        else:
            log(f"checando {len(sessions_all)} sessions com {MAX_WORKERS} workers...")
            sessions_active = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(session_has_recent_steps, http, s.get("id"), agora): s
                    for s in sessions_all
                }
                for future in as_completed(futures):
                    session = futures[future]
                    try:
                        if future.result():
                            sessions_active.append(session)
                    except Exception:
                        pass

            log(f"sessions com atividade recente: {len(sessions_active)}")

            usuarios_agora = {}
            for session in sessions_active:
                user = session.get("user") or {}
                application = session.get("application") or {}
                username = user.get("username")
                fullname = user.get("fullName")
                app_name = application.get("name")
                session_id = session.get("id")
                start_dt = session.get("startDatetime")

                user_key = username or fullname or "unknown"
                info = usuarios_agora.setdefault(user_key, {
                    "user_username": username,
                    "user_fullName": fullname,
                    "applications": [],
                    "session_ids": [],
                    "sessions_count": 0,
                    "first_session_start": None,
                })

                if app_name and app_name not in info["applications"]:
                    info["applications"].append(app_name)
                info["session_ids"].append(session_id)
                info["sessions_count"] += 1
                if start_dt is not None and (info["first_session_start"] is None or start_dt < info["first_session_start"]):
                    info["first_session_start"] = start_dt
    finally:
        http.close()

    usuarios_antes = read_state()

    entrou = set(usuarios_agora.keys()) - set(usuarios_antes.keys())
    saiu = set(usuarios_antes.keys()) - set(usuarios_agora.keys())
    continua = set(usuarios_agora.keys()) & set(usuarios_antes.keys())

    total_docs = 0
    for user_key in entrou | continua:
        info = usuarios_agora[user_key]
        status = "ENTROU" if user_key in entrou else "ATIVO"
        doc = {
            "run_time_ms": run_time_ms,
            "user_key": user_key,
            "user_username": info["user_username"],
            "user_fullName": info["user_fullName"],
            "applications": info["applications"],
            "session_ids": [str(s) for s in info["session_ids"]],
            "sessions_count": info["sessions_count"],
            "first_session_start": info["first_session_start"],
            "status": status,
        }
        doc["_doc_id"] = make_doc_id(run_time_ms, user_key, status)
        emit(doc)
        total_docs += 1

    for user_key in saiu:
        info = usuarios_antes.get(user_key, {})
        doc = {
            "run_time_ms": run_time_ms,
            "user_key": user_key,
            "user_username": info.get("user_username"),
            "user_fullName": info.get("user_fullName"),
            "applications": info.get("applications", []),
            "session_ids": [],
            "sessions_count": 0,
            "first_session_start": None,
            "status": "SAIU",
        }
        doc["_doc_id"] = make_doc_id(run_time_ms, user_key, "SAIU")
        emit(doc)
        total_docs += 1

    sys.stdout.flush()

    write_state(usuarios_agora)

    log(f"fim run: entrou={len(entrou)} saiu={len(saiu)} ativo_continua={len(continua)} docs_emitidos={total_docs}")


if __name__ == "__main__":
    main()
