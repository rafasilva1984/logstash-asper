#!/usr/bin/env python3
"""
Worker do pipeline sws-recordings (fonte: API Alero).

Le recordings + sessionSteps da API da Alero e emite NDJSON no stdout,
um documento por step (ou um documento sintetico para recording sem step
algum). Logs de diagnostico vao para stderr — nunca para stdout, que e o
NDJSON consumido pelo Logstash (input exec + codec json_lines).

Dois modos (--mode), pensados para rodar como DOIS pipelines Logstash
separados, com o MESMO worker:

  --mode realtime   janela pequena, com checkpoint + overlap (roda a cada
                     poucos segundos/minutos). E o fluxo principal.

  --mode reconcile   janela fixa e larga (ex.: ultimas 48h), sem checkpoint,
                     rodando 1x/dia via `schedule`. Existe porque a API da
                     Alero filtra recordings pelo INICIO da sessao e so
                     devolve sessao FECHADA: uma sessao longa que comecou
                     fora da janela realtime pode nunca ser vista por ela.
                     O reconcile varre de novo um intervalo largo; como o
                     _id e deterministico e o output usa action=create,
                     tudo que ja foi indexado vira 409 (dedup benigno).
"""

import argparse
import hashlib
import json
import os
import sys
import threading
import time
import urllib3

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

urllib3.disable_warnings()

PIPELINE_NAME = "sws-recordings"


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

LIMIT = _env("ALERO_PAGE_LIMIT", 100, int)
MAX_WORKERS = _env("ALERO_MAX_WORKERS", 10, int)
MAX_RETRIES = _env("ALERO_MAX_RETRIES", 5, int)
MAX_RESULT_WINDOW = 10000  # teto de offset+limit (max_result_window do ES por tras da Alero)

CHECKPOINT_FILE = _env("SWS_CHECKPOINT_FILE", "/var/lib/logstash/sws-recordings/checkpoint")
OVERLAP_MINUTES = _env("SWS_OVERLAP_MINUTES", 5, int)
BACKFILL_MINUTES = _env("SWS_BACKFILL_MINUTES", 15, int)
MAX_WINDOW_MINUTES = _env("SWS_MAX_WINDOW_MINUTES", 120, int)
MAX_SLICES_PER_RUN = _env("SWS_MAX_SLICES_PER_RUN", 12, int)
RECONCILE_LOOKBACK_HOURS = _env("SWS_RECONCILE_LOOKBACK_HOURS", 48, int)


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sys.stderr.write(f"[{ts}] {msg}\n")
    sys.stderr.flush()


# ------------------------------------------------------------------ auth ---

class TokenManager:
    """Thread-safe: o refresh acontece dentro do lock pra nao ter corrida
    quando varios workers baterem em get_token() ao mesmo tempo."""

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


def request_retry(url, params=None, max_attempts=None):
    """Retry robusto: 429 (respeita Retry-After), 401 (refresh de token),
    5xx e erros de rede com backoff exponencial (teto 30s) -> transiente,
    retenta. 4xx nao recuperavel propaga na hora -> falha permanente."""
    max_attempts = max_attempts or MAX_RETRIES
    last_error = None

    for attempt in range(max_attempts):
        try:
            r = requests.get(url, headers=auth_headers(), params=params, verify=False, timeout=30)

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


# --------------------------------------------------------------- helpers ---

def first_not_none(*values):
    for v in values:
        if v is not None:
            return v
    return None


def extract_value_old(step):
    return first_not_none(step.get("elementOldValue"), step.get("value_old"), step.get("valueOld"), step.get("oldValue"))


def extract_value_new(step):
    return first_not_none(step.get("elementValue"), step.get("value_new"), step.get("valueNew"), step.get("newValue"))


def make_doc_id(*parts):
    base = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


# ----------------------------------------------------------- coleta API ---

def _collect_window(from_time, to_time, seen_ids, recordings):
    """Coleta uma janela de tempo por offset. Se a janela passar de
    MAX_RESULT_WINDOW (teto de offset+limit), divide ao meio no tempo e
    recorre nas duas metades — dedup por id cobre qualquer sobreposicao."""
    offset = 0
    url = f"{BASE_URL}/sws/recordings"

    while offset + LIMIT <= MAX_RESULT_WINDOW:
        params = {"fromTime": from_time, "toTime": to_time, "offset": offset, "limit": LIMIT}
        data = request_retry(url, params=params)
        batch = data.get("recordings", [])

        if not batch:
            return

        for rec in batch:
            rid = rec.get("id")
            if rid not in seen_ids:
                seen_ids.add(rid)
                recordings.append(rec)

        if len(batch) < LIMIT:
            return
        offset += LIMIT
    else:
        if to_time - from_time <= 1:
            log(f"!! janela minima ({from_time}) ainda >{MAX_RESULT_WINDOW}; possivel perda de dado")
            return
        mid = (from_time + to_time) // 2
        log(f"  janela >{MAX_RESULT_WINDOW}, dividindo (~{(to_time - from_time) / 3_600_000:.1f}h) ...")
        _collect_window(from_time, mid, seen_ids, recordings)
        _collect_window(mid + 1, to_time, seen_ids, recordings)


def collect_recordings(start_time, end_time):
    recordings = []
    seen_ids = set()
    _collect_window(start_time, end_time, seen_ids, recordings)
    return recordings


def get_recording_steps(recording_id):
    """Pagina sessionSteps por offset. Guarda anti-loop: se o endpoint
    ignorar o offset e devolver sempre a mesma pagina, para (senao o dedup
    por step id zera os 'novos' e a coleta nunca termina)."""
    steps = []
    seen = set()
    offset = 0

    while True:
        url = f"{BASE_URL}/sws/recordings/{recording_id}"
        data = request_retry(url, params={"offset": offset, "limit": LIMIT})
        batch = data.get("sessionSteps", [])
        if not batch:
            break

        novos = 0
        for s in batch:
            sid = s.get("id")
            if sid is None or sid not in seen:
                if sid is not None:
                    seen.add(sid)
                steps.append(s)
                novos += 1

        if len(batch) < LIMIT or novos == 0:
            break
        offset += LIMIT

    return steps


# ------------------------------------------------------------- flatten ----

def flatten_recording_steps(recording, steps, extraction_status, extraction_error, steps_capturados):
    user = recording.get("user") or {}
    application = recording.get("application") or {}
    tab_ids = recording.get("tabIds")
    recording_id = recording.get("id")

    base = {
        "recording_id": recording_id,
        "user_id": user.get("id"),
        "user_fullName": user.get("fullName"),
        "user_username": user.get("username"),
        "application_id": application.get("id"),
        "application_name": application.get("name"),
        "recording_startDatetime": recording.get("startDatetime"),
        "recording_endDatetime": recording.get("endDatetime"),
        "recording_durationSeconds": recording.get("durationSeconds"),
        "recording_stepsCount": recording.get("stepsCount"),
        "recording_flag": recording.get("flag"),
        "recording_tabIds": [str(t) for t in tab_ids] if tab_ids else None,
        "extraction_status": extraction_status,
        "extraction_error": extraction_error,
        "steps_capturados": steps_capturados,
    }

    fallback_time = first_not_none(recording.get("startDatetime"), recording.get("endDatetime"))
    rows = []

    if not steps:
        row = dict(base)
        row["event_time_ms"] = fallback_time
        row["_doc_id"] = make_doc_id(recording_id, "no_steps")
        rows.append(row)
        return rows

    for idx, step in enumerate(steps):
        step_type = step.get("type")
        value_old = extract_value_old(step)
        value_new = extract_value_new(step)
        step_time = first_not_none(step.get("datetime"), fallback_time)

        row = dict(base)
        row.update({
            "step_index": idx,
            "step_id": step.get("id"),
            "step_datetime": step.get("datetime"),
            "step_extensionEventId": step.get("extensionEventId"),
            "step_flag": step.get("flag"),
            "step_tabId": step.get("tabId"),
            "step_type": step_type,
            "step_url": step.get("url"),
            "step_windowTitle": step.get("windowTitle"),
            "step_windowHeight": step.get("windowHeight"),
            "step_windowWidth": step.get("windowWidth"),
            "step_clientTimeZone": step.get("clientTimeZone"),
            "step_elementXPath": step.get("elementXPath"),
            "step_pressedKey": step.get("pressedKey"),
            "step_cursorX": step.get("cursorX"),
            "step_cursorY": step.get("cursorY"),
            "step_elementTag": step.get("elementTag"),
            "step_elementText": step.get("elementText"),
            "step_elementValue": step.get("elementValue"),
            "step_elementOldValue": step.get("elementOldValue"),
            "value_old": value_old,
            "value_new": value_new,
            "is_value_change": str(step_type).lower() == "valuechange",
            "is_mouse_click": str(step_type).lower() == "mouseclick",
            "is_navigated_to": str(step_type).lower() == "navigatedto",
            "is_clipboard_paste": str(step_type).lower() == "clipboardpasted",
            "event_time_ms": step_time,
        })
        row["_doc_id"] = make_doc_id(recording_id, first_not_none(step.get("id"), idx), step_type, step_time)
        rows.append(row)

    return rows


def process_recording(recording):
    recording_id = recording.get("id")
    expected = recording.get("stepsCount")

    try:
        steps = get_recording_steps(recording_id)
        captured = len(steps)
        status = "INCOMPLETO" if (expected is not None and captured < expected) else "OK"
        return flatten_recording_steps(recording, steps, status, None, captured)
    except Exception as e:
        log(f"erro recording {recording_id}: {e}")
        return flatten_recording_steps(recording, [], "ERRO_EXTRACAO", str(e)[:300], 0)


# --------------------------------------------------------------- output ---

def emit(doc):
    sys.stdout.write(json.dumps(doc, ensure_ascii=False, default=str) + "\n")


def emit_heartbeat():
    sys.stdout.write(json.dumps({"heartbeat": PIPELINE_NAME}) + "\n")
    sys.stdout.flush()


# ----------------------------------------------------------- checkpoint ---

def read_checkpoint():
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            iso = f.read().strip()
        if not iso:
            return None
        return int(datetime.fromisoformat(iso).timestamp() * 1000)
    except FileNotFoundError:
        return None
    except Exception as e:
        log(f"checkpoint ilegivel ({e}), tratando como inexistente")
        return None


def write_checkpoint(ms):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    iso = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as f:
        f.write(iso)
    os.replace(tmp, CHECKPOINT_FILE)


# --------------------------------------------------------------- janela ---

def build_slices(mode):
    """Monta a lista de janelas [from_ms, to_ms) a varrer nesta execucao.

    reconcile: uma unica janela larga e fixa (agora - LOOKBACK -> agora),
    sem depender de checkpoint.

    realtime: unificado com o backfill inicial (mesma logica do worker de
    loginprodesp) — sem checkpoint, comeca em agora-BACKFILL_MINUTES; com
    checkpoint, comeca em checkpoint-OVERLAP_MINUTES. Fatia em pedacos de
    MAX_WINDOW_MINUTES, no maximo MAX_SLICES_PER_RUN por execucao, pra
    nunca segurar o processo por tempo indefinido numa unica chamada."""
    now_ms = int(time.time() * 1000)

    if mode == "reconcile":
        start = now_ms - RECONCILE_LOOKBACK_HOURS * 3600_000
        return [(start, now_ms)], False

    checkpoint = read_checkpoint()
    start = (now_ms - BACKFILL_MINUTES * 60_000) if checkpoint is None else (checkpoint - OVERLAP_MINUTES * 60_000)

    if start >= now_ms:
        return [], True

    window_ms = MAX_WINDOW_MINUTES * 60_000
    slices = []
    cursor = start
    while cursor < now_ms and len(slices) < MAX_SLICES_PER_RUN:
        end = min(cursor + window_ms, now_ms)
        slices.append((cursor, end))
        cursor = end

    return slices, True


def _fmt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def run_slice(from_ms, to_ms):
    log(f"janela {_fmt(from_ms)} -> {_fmt(to_ms)}")

    recordings = collect_recordings(from_ms, to_ms)
    log(f"recordings coletadas: {len(recordings)}")

    total_docs = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_recording, r) for r in recordings]
        for future in as_completed(futures):
            for row in future.result():
                emit(row)
                total_docs += 1

    log(f"documentos emitidos nesta janela: {total_docs}")


# ------------------------------------------------------------------ main --

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["realtime", "reconcile"], default="realtime")
    args = parser.parse_args()

    if not CLIENT_ID or not CLIENT_SECRET:
        log("ALERO_CLIENT_ID / ALERO_CLIENT_SECRET nao configurados")
        emit_heartbeat()
        return

    try:
        slices, uses_checkpoint = build_slices(args.mode)
    except Exception as e:
        log(f"erro montando janelas: {e}")
        emit_heartbeat()
        return

    if not slices:
        log("nada a fazer (checkpoint ja alcancou o presente)")
        emit_heartbeat()
        return

    for from_ms, to_ms in slices:
        try:
            run_slice(from_ms, to_ms)
        except Exception as e:
            log(f"falha na janela {_fmt(from_ms)}-{_fmt(to_ms)}: {e} "
                f"— checkpoint NAO avanca, retoma nesta janela na proxima execucao")
            break
        else:
            if uses_checkpoint:
                write_checkpoint(to_ms)

    emit_heartbeat()
    sys.stdout.flush()


if __name__ == "__main__":
    main()
