#!/usr/bin/env python3
"""
Push manual do NDJSON do detran_steps_worker.py pro Elasticsearch,
replicando o filter+output do detran-steps-daily.conf (event_time_ms ->
@timestamp, remove _doc_id/event_time_ms, action=create no data stream)
SEM passar pelo Logstash.

Existe para a carga imediata (sem esperar as 07:00 nem reiniciar o
Logstash) — mesmo padrao do functional_push.py dos *-directory.

Uso:
    set -a; source /etc/logstash/envio_detran_steps.env; set +a
    python3 detran_steps_push.py < /tmp/detran_steps.ndjson

Ignora linhas sem "_doc_id" (heartbeat — mesmo criterio do .conf). Usa a
_bulk API com op "create": rerun = 409, dedup benigno. Seguro reexecutar
sobre o mesmo arquivo.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

ELASTIC_URL = os.environ["ELASTIC_URL"].rstrip("/")
AUTH = (os.environ["ELASTIC_USER"], os.environ["ELASTIC_PASSWORD"])
INDEX = os.environ.get("DETRAN_STEPS_DATA_STREAM", "logs-detran.steps-default")
BATCH_SIZE = int(os.environ.get("DETRAN_STEPS_PUSH_BATCH_SIZE", "2000"))


def ms_to_iso(ms):
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def to_action_and_doc(line):
    doc = json.loads(line)
    if "_doc_id" not in doc:
        return None
    doc_id = doc.pop("_doc_id")
    event_ms = doc.pop("event_time_ms", None)
    if event_ms is not None:
        doc["@timestamp"] = ms_to_iso(event_ms)
    return {"create": {"_index": INDEX, "_id": doc_id}}, doc


def flush(batch):
    if not batch:
        return 0, 0, 0
    body = "\n".join(json.dumps(part, ensure_ascii=False, default=str)
                     for pair in batch for part in pair) + "\n"
    for attempt in range(5):
        try:
            r = requests.post(f"{ELASTIC_URL}/_bulk", data=body.encode("utf-8"),
                              headers={"Content-Type": "application/x-ndjson"},
                              auth=AUTH, timeout=120)
        except requests.RequestException as e:
            sys.stderr.write(f"[push] erro de rede ({e}), tentativa {attempt + 1}/5\n")
            time.sleep(min(2 ** attempt, 30))
            continue
        if r.status_code in (429, 502, 503, 504):
            sys.stderr.write(f"[push] HTTP {r.status_code}, tentativa {attempt + 1}/5\n")
            time.sleep(min(2 ** attempt, 30))
            continue
        if r.status_code != 200:
            sys.stderr.write(f"[erro _bulk] HTTP {r.status_code}: {r.text[:1000]}\n")
            r.raise_for_status()
        break
    else:
        raise RuntimeError("_bulk falhou apos 5 tentativas")

    created = duplicate = failed = 0
    for item in r.json().get("items", []):
        result = item.get("create", {})
        status = result.get("status")
        if status in (200, 201):
            created += 1
        elif status == 409:
            duplicate += 1
        else:
            failed += 1
            reason = result.get("error", {}).get("reason", "?")
            sys.stderr.write(f"[erro] _id={result.get('_id')} status={status} reason={reason}\n")
    return created, duplicate, failed


def main():
    totals = [0, 0, 0]
    skipped = 0
    batch = []
    t0 = time.time()

    def do_flush():
        c, d, f = flush(batch)
        totals[0] += c
        totals[1] += d
        totals[2] += f
        sys.stderr.write(f"[push] lote: created={c} dup={d} failed={f} "
                         f"(acumulado created={totals[0]} dup={totals[1]} failed={totals[2]})\n")
        batch.clear()

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        pair = to_action_and_doc(line)
        if pair is None:
            skipped += 1
            continue
        batch.append(pair)
        if len(batch) >= BATCH_SIZE:
            do_flush()
    if batch:
        do_flush()

    sys.stderr.write(f"\n[resumo] created={totals[0]} duplicate_409={totals[1]} failed={totals[2]} "
                     f"skipped_sem_doc_id={skipped} tempo={time.time() - t0:.1f}s indice={INDEX}\n")
    if totals[2]:
        sys.exit(1)


if __name__ == "__main__":
    main()
