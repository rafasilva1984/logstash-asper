#!/usr/bin/env python3
"""
Worker do pipeline sgp-steps-daily (fonte: API Alero, tenant SGP).

Porta o script de execucao diaria do SGP (que gerava
saida_sgp/steps_unificados.xlsx) para a arquitetura da plataforma: em vez
do Excel, emite NDJSON no stdout — um documento por sessionStep (ou um
documento sintetico para sessao sem step algum). Logs vao para stderr,
nunca para stdout (CLAUDE.md §2). Codec json_lines + heartbeat (3.1/3.2):
24h de steps e volume grande.

Agendado 1x/dia (07:00, hora local do servidor) via `schedule` no input
exec. Janela padrao = ultimas 24h de ABERTURA de sessao (fromTime/toTime
da Alero filtram startDatetime em ms, inclusivos).

Diferencas deliberadas em relacao ao script original:
  - Checkpoint: a janela vai de (checkpoint - overlap) ate agora. Na
    operacao normal (1 execucao/dia) isso da 24h + overlap. Se uma
    execucao falhar ou o Logstash estiver parado as 07:00, a proxima cobre
    o buraco em vez de perde-lo (o original avisava: "atrasos no disparo
    podem deixar intervalos sem cobertura"). Recuperacao limitada a
    SGP_STEPS_MAX_CATCHUP_HOURS, em fatias de no maximo 24h (CLAUDE.md 3.6).
  - Overlap: a Alero so devolve sessao FECHADA filtrando pelo INICIO
    (CLAUDE.md 3.12). Sessao aberta pouco antes do disparo e ainda em
    andamento escaparia para sempre; o overlap revarre a borda. Como o _id
    e deterministico e o output usa action=create, o que ja foi indexado
    vira 409 (dedup benigno).
  - Sessao com falha de extracao NAO e gravada como vazia: a fatia nao
    avanca o checkpoint e a proxima execucao retenta (3.7).

Modo manual (carga imediata / reprocesso), sem ler nem gravar checkpoint:
  --since 2026-10-01T00:00:00-03:00 [--until 2026-10-02T00:00:00-03:00]
"""

import argparse
import hashlib
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import quote

import requests
import urllib3

PIPELINE_NAME = "sgp-steps-daily"


# ---------------------------------------------------------------- config ---

def _env(name, default=None, cast=str):
    val = os.environ.get(name)
    if val is None or val == "":
        return default
    return cast(val)


# Prefixo SGP_STEPS_ em tudo: os .env de todos os pipelines entram no MESMO
# processo do Logstash (EnvironmentFile da unit). ALERO_CLIENT_ID ja e usado
# pelo sws-recordings com outra credencial — reusar o nome sobrescreveria.
AUTH_URL = _env("SGP_STEPS_AUTH_URL", "https://auth.alero.io/auth/realms/serviceaccounts/protocol/openid-connect/token")
BASE_URL = _env("SGP_STEPS_BASE_URL", "https://api.alero.io/v2-edge")
CLIENT_ID = _env("SGP_STEPS_CLIENT_ID")
CLIENT_SECRET = _env("SGP_STEPS_CLIENT_SECRET")
FREE_SEARCH = _env("SGP_STEPS_FREE_SEARCH", "sgp")

LIMIT = _env("SGP_STEPS_PAGE_LIMIT", 100, int)
MAX_WORKERS = _env("SGP_STEPS_MAX_WORKERS", 10, int)
MAX_RETRIES = _env("SGP_STEPS_MAX_RETRIES", 5, int)

CHECKPOINT_FILE = _env("SGP_STEPS_CHECKPOINT_FILE", "/var/lib/logstash/sgp-steps/checkpoint")
LOOKBACK_HOURS = _env("SGP_STEPS_LOOKBACK_HOURS", 24, int)
OVERLAP_MINUTES = _env("SGP_STEPS_OVERLAP_MINUTES", 60, int)
MAX_CATCHUP_HOURS = _env("SGP_STEPS_MAX_CATCHUP_HOURS", 168, int)
SLICE_HOURS = 24  # teto por fatia (CLAUDE.md 3.6)

# Original desligava a verificacao de TLS; mantido como default.
# SGP_STEPS_VERIFY_TLS=true valida; SGP_STEPS_CA_BUNDLE=<pem> para CA corporativa.
VERIFY_TLS = _env("SGP_STEPS_CA_BUNDLE") or (_env("SGP_STEPS_VERIFY_TLS", "false").lower() in ("1", "true", "yes"))
if VERIFY_TLS is False:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sys.stderr.write(f"[{ts}] {msg}\n")
    sys.stderr.flush()


def _fmt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


# ------------------------------------------------------------------ auth ---

class TokenManager:
    """Thread-safe: refresh dentro do lock, sem corrida entre threads."""

    def __init__(self):
        self.token = None
        self.expiry = 0
        self._lock = threading.Lock()

    def get_token(self):
        with self._lock:
            if self.token and time.monotonic() < self.expiry:
                return self.token
            r = requests.post(AUTH_URL, data={
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type": "client_credentials",
                "scope": "openid",
            }, timeout=30, verify=VERIFY_TLS)
            if r.status_code != 200:
                raise RuntimeError(f"Autenticacao HTTP {r.status_code}: {r.text[:200]}")
            data = r.json()
            self.token = data["access_token"]
            self.expiry = time.monotonic() + max(1, float(data["expires_in"]) - 30)
            return self.token

    def invalidate(self, rejected):
        with self._lock:
            if self.token == rejected:
                self.expiry = 0


token_manager = TokenManager()


def request_json(url, params=None):
    """Transiente (rede, 401, 429, 5xx) -> retenta com backoff; outro 4xx ->
    falha permanente na hora (CLAUDE.md 3.7). SSL nao retenta: e config."""
    last_error = None
    for attempt in range(MAX_RETRIES):
        token = token_manager.get_token()
        try:
            r = requests.get(url, headers={"Authorization": f"Bearer {token}"},
                             params=params, timeout=30, verify=VERIFY_TLS)
        except requests.exceptions.SSLError as e:
            raise RuntimeError(f"TLS: {e}; verifique SGP_STEPS_VERIFY_TLS/SGP_STEPS_CA_BUNDLE") from None
        except requests.RequestException as e:
            last_error = f"{type(e).__name__}: {e}"
            time.sleep(min(2 ** attempt, 30))
            continue

        if r.status_code == 200:
            data = r.json()
            if not isinstance(data, dict):
                raise RuntimeError("Resposta da API nao e um objeto JSON")
            return data
        if r.status_code == 401:
            token_manager.invalidate(token)
            last_error = "401 nao autorizado"
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            try:
                delay = min(60, max(0, float(r.headers.get("Retry-After", 2 ** attempt))))
            except ValueError:
                delay = min(2 ** attempt, 30)
            last_error = f"HTTP {r.status_code}"
            time.sleep(delay)
            continue
        raise RuntimeError(f"Erro permanente HTTP {r.status_code}: {r.text[:200]}")

    raise RuntimeError(f"Falha apos {MAX_RETRIES} tentativas: {last_error}")


# ----------------------------------------------------------- coleta API ---

def collect_recordings(start_ms, end_ms):
    """Mesma estrategia do script original: nao depende da ordem da
    resposta; janela saturada (>= LIMIT) e dividida ao meio no tempo, em vez
    de avancar last_start+1 (que pode pular IDs empatados no mesmo ms)."""
    pending = [(start_ms, end_ms)]
    found = {}
    queries = 0
    while pending:
        lo, hi = pending.pop()
        queries += 1
        data = request_json(f"{BASE_URL}/sws/recordings", {
            "fromTime": lo, "toTime": hi, "limit": LIMIT, "freeSearch": FREE_SEARCH})
        batch = data.get("recordings")
        if not isinstance(batch, list):
            raise RuntimeError("Resposta sem lista recordings; janela incompleta")
        if len(batch) >= LIMIT:
            if lo >= hi:
                raise RuntimeError(f"{LIMIT}+ sessoes no mesmo ms ({lo}): necessario cursor oficial da API")
            mid = (lo + hi) // 2
            pending.extend([(lo, mid), (mid + 1, hi)])
            continue
        for rec in batch:
            rid = rec.get("id")
            if rid is None:
                raise RuntimeError("Recording sem ID")
            opened = rec.get("startDatetime")
            if isinstance(opened, bool) or opened is None or not lo <= int(opened) <= hi:
                raise RuntimeError("API nao respeitou filtro de abertura; validar fromTime/toTime")
            found[str(rid)] = rec
    log(f"  listagem: sessoes={len(found)} consultas={queries}")
    return list(found.values())


def get_recording_steps(recording_id):
    data = request_json(f"{BASE_URL}/sws/recordings/{quote(str(recording_id), safe='')}")
    if data.get("hasMore") or data.get("nextCursor") or data.get("nextPage"):
        raise RuntimeError("Detalhes paginados: implementar paginacao oficial antes de coletar")
    steps = data.get("sessionSteps")
    if not isinstance(steps, list):
        raise RuntimeError("Resposta sem lista sessionSteps; nao tratar como sessao vazia")
    return steps


# ------------------------------------------------------------- flatten ----

def first_not_none(*values):
    for v in values:
        if v is not None:
            return v
    return None


def step_key(recording_id, step):
    """_id do step — identica ao event_uid do script original."""
    if step.get("id") is not None:
        identity = [str(recording_id), "id", str(step["id"])]
    elif step.get("extensionEventId") is not None:
        identity = [str(recording_id), "event", str(step["extensionEventId"]),
                    step.get("tabId"), step.get("datetime")]
    else:
        # Fallback deterministico; eventos identicos sem IDs sao indistinguiveis.
        identity = [str(recording_id), "payload", step]
    return hashlib.sha256(json.dumps(identity, sort_keys=True, ensure_ascii=False,
                                     default=str).encode("utf-8")).hexdigest()


def recording_base(recording, status, steps_received, elapsed):
    user = recording.get("user") or {}
    application = recording.get("application") or {}
    tab_ids = recording.get("tabIds")
    return {
        "recording_id": recording.get("id"),
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
        "extraction_status": status,
        "steps_recebidos": steps_received,
        "tempo_processamento_sessao_s": elapsed,
    }


def step_fields(step):
    step_type = step.get("type")
    lowered = str(step_type).lower()
    return {
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
        "value_old": first_not_none(step.get("elementOldValue"), step.get("value_old"),
                                    step.get("valueOld"), step.get("oldValue")),
        "value_new": first_not_none(step.get("elementValue"), step.get("value_new"),
                                    step.get("valueNew"), step.get("newValue")),
        "is_value_change": lowered == "valuechange",
        "is_mouse_click": lowered == "mouseclick",
        "is_navigated_to": lowered == "navigatedto",
        "is_clipboard_paste": lowered == "clipboardpasted",
    }


def process_recording(recording):
    """Retorna (docs, ok). ok=False -> extracao falhou; nenhum doc e
    emitido para a sessao (nao gravar como vazia) e a fatia nao avanca."""
    started = time.monotonic()
    rid = recording["id"]
    try:
        steps = get_recording_steps(rid)
    except Exception as e:
        log(f"  FALHA sessao {rid}: {e}")
        return [], False

    elapsed = round(time.monotonic() - started, 3)
    fallback_time = first_not_none(recording.get("startDatetime"), recording.get("endDatetime"))

    if not steps:
        doc = recording_base(recording, "SEM_STEPS", 0, elapsed)
        doc["event_time_ms"] = fallback_time
        doc["_doc_id"] = hashlib.sha256(f"{rid}|no_steps".encode("utf-8")).hexdigest()
        return [doc], True

    base = recording_base(recording, "SUCESSO", len(steps), elapsed)
    docs = []
    seen = set()
    for index, step in enumerate(steps):
        uid = step_key(rid, step)
        if uid in seen:
            continue
        seen.add(uid)
        doc = dict(base)
        doc.update(step_fields(step))
        doc["step_index"] = index
        doc["event_uid"] = uid
        doc["event_time_ms"] = first_not_none(step.get("datetime"), fallback_time)
        doc["_doc_id"] = uid
        docs.append(doc)
    return docs, True


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
        return int(datetime.fromisoformat(iso).timestamp() * 1000) if iso else None
    except FileNotFoundError:
        return None
    except Exception as e:
        log(f"checkpoint ilegivel ({e}), tratando como inexistente")
        return None


def write_checkpoint(ms):
    """Atomico (.tmp + rename) e monotonico: nunca retrocede."""
    current = read_checkpoint()
    if current is not None and ms <= current:
        return
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as f:
        f.write(_fmt(ms))
    os.replace(tmp, CHECKPOINT_FILE)


# --------------------------------------------------------------- janela ---

def parse_ts(value):
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise argparse.ArgumentTypeError(f"informe o fuso no timestamp: {value} (ex.: -03:00)")
    return int(dt.timestamp() * 1000)


def build_slices(now_ms, since_ms=None, until_ms=None):
    """Retorna fatias [(from_ms, to_ms)] de no maximo SLICE_HOURS."""
    if since_ms is not None:
        start, end = since_ms, (until_ms if until_ms is not None else now_ms)
    else:
        end = now_ms
        checkpoint = read_checkpoint()
        if checkpoint is None:
            start = now_ms - LOOKBACK_HOURS * 3_600_000
        else:
            start = checkpoint - OVERLAP_MINUTES * 60_000
        floor = now_ms - MAX_CATCHUP_HOURS * 3_600_000
        if start < floor:
            log(f"!! checkpoint muito antigo; janela limitada a {MAX_CATCHUP_HOURS}h "
                f"(perde-se o intervalo {_fmt(start)} -> {_fmt(floor)})")
            start = floor

    slices = []
    cursor = start
    while cursor < end:
        stop = min(cursor + SLICE_HOURS * 3_600_000, end)
        slices.append((cursor, stop))
        cursor = stop + 1  # toTime e inclusivo
    return slices


def run_slice(from_ms, to_ms):
    """Retorna (docs_emitidos, falhas)."""
    log(f"janela de abertura {_fmt(from_ms)} -> {_fmt(to_ms)}")
    recordings = collect_recordings(from_ms, to_ms)

    total_docs = 0
    failures = 0
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_recording, r) for r in recordings]
        for future in as_completed(futures):
            docs, ok = future.result()
            if not ok:
                failures += 1
            for doc in docs:
                emit(doc)
            total_docs += len(docs)
            done += 1
            if done % 200 == 0:
                log(f"  progresso {done}/{len(recordings)} sessoes | docs={total_docs} | falhas={failures}")

    log(f"  fim da janela: sessoes={len(recordings)} docs={total_docs} falhas={failures}")
    return total_docs, failures


# ------------------------------------------------------------------ main --

def main():
    parser = argparse.ArgumentParser(description="Worker sgp-steps-daily (Alero -> NDJSON)")
    parser.add_argument("--since", type=parse_ts,
                        help="modo manual: inicio da janela de abertura (ISO com fuso); nao usa checkpoint")
    parser.add_argument("--until", type=parse_ts,
                        help="modo manual: fim da janela (default: agora)")
    args = parser.parse_args()
    if args.until is not None and args.since is None:
        parser.error("--until exige --since")

    started = time.monotonic()
    manual = args.since is not None

    if not CLIENT_ID or not CLIENT_SECRET:
        log("SGP_STEPS_CLIENT_ID / SGP_STEPS_CLIENT_SECRET nao configurados")
        emit_heartbeat()
        return

    now_ms = int(time.time() * 1000)
    slices = build_slices(now_ms, args.since, args.until)
    log(f"inicio | modo={'manual' if manual else 'agendado'} | fatias={len(slices)} | freeSearch={FREE_SEARCH}")

    total_docs = 0
    for from_ms, to_ms in slices:
        try:
            docs, failures = run_slice(from_ms, to_ms)
        except Exception as e:
            log(f"falha na janela {_fmt(from_ms)} -> {_fmt(to_ms)}: {e} "
                f"— checkpoint NAO avanca, retoma na proxima execucao")
            break
        total_docs += docs
        if failures:
            log(f"{failures} sessoes falharam — checkpoint NAO avanca, "
                f"proxima execucao revarre esta janela (ja indexados viram 409)")
            break
        if not manual:
            write_checkpoint(to_ms)

    sys.stdout.flush()
    log(f"fim | docs={total_docs} | tempo={time.monotonic() - started:.1f}s")
    emit_heartbeat()


if __name__ == "__main__":
    main()
