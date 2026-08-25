#!/usr/bin/env python3
"""
Push manual do NDJSON do cyberark_prodesp_worker.py pro Elasticsearch,
replicando a logica do filter+output do cyberark-prodesp-directory.conf
(snapshot_ms -> @timestamp, remove _doc_id/snapshot_ms, action=create na
data stream) SEM passar pelo Logstash.

Existe pra permitir teste funcional imediato contra o Elastic real, sem
depender do exec+schedule (que foi o que falhou em silencio no incidente
2026-08-24/25, ver DEPLOY.md) e sem esperar o horario noturno.

Uso:
    set -a; source /etc/logstash/envio_cyberark_prodesp.env; set +a
    python3 /etc/logstash/pipelines/cyberark_prodesp_worker.py 2>/tmp/worker.log \
      | python3 functional_push.py

Le NDJSON do stdin, ignora linhas sem "_doc_id" (heartbeat/invalidas —
mesmo criterio do "if ![_doc_id] { drop {} }" do .conf), usa a _bulk API
com op "create" (mesma semantica do output elasticsearch do Logstash:
action=>create + document_id deterministico -> rerun = 409, dedup
benigno).
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

ELASTIC_URL = os.environ["ELASTIC_URL"].rstrip("/")
ELASTIC_USER = os.environ["ELASTIC_USER"]
ELASTIC_PASSWORD = os.environ["ELASTIC_PASSWORD"]
INDEX = os.environ.get("PRODESP_DATA_STREAM", "logs-cyberark.prodespdirectory-default")
BATCH_SIZE = int(os.environ.get("FUNCTIONAL_PUSH_BATCH_SIZE", "2000"))

AUTH = (ELASTIC_USER, ELASTIC_PASSWORD)


def ms_to_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def to_action_and_doc(line):
    """Mesma transformacao do filter do .conf: dropa sem _doc_id, monta
    @timestamp a partir de snapshot_ms (equivalente ao filtro date com
    UNIX_MS), remove os campos que o .conf remove."""
    doc = json.loads(line)
    if "_doc_id" not in doc:
        return None
    doc_id = doc.pop("_doc_id")
    snapshot_ms = doc.pop("snapshot_ms", None)
    if snapshot_ms is not None:
        doc["@timestamp"] = ms_to_iso(snapshot_ms)
    return {"create": {"_index": INDEX, "_id": doc_id}}, doc


def flush(batch):
    if not batch:
        return 0, 0, 0
    body = "\n".join(
        json.dumps(part, ensure_ascii=False, default=str) for pair in batch for part in pair
    ) + "\n"
    r = requests.post(
        f"{ELASTIC_URL}/_bulk",
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        auth=AUTH,
        timeout=120,
    )
    if r.status_code != 200:
        # erro no nivel do _bulk inteiro (index inexistente, auth, payload
        # malformado, etc.) -- nao confundir com erro por-item (ex.: 409 de
        # dedup, tratado abaixo). Corpo da resposta e o que da o motivo real
        # (raise_for_status() sozinho so mostra o status code).
        sys.stderr.write(f"[erro _bulk] HTTP {r.status_code}: {r.text[:1000]}\n")
        r.raise_for_status()
    resp = r.json()

    created = duplicate = failed = 0
    for item in resp.get("items", []):
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
    total_created = total_dup = total_failed = total_skipped = 0
    batch = []
    t0 = time.time()

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        pair = to_action_and_doc(line)
        if pair is None:
            total_skipped += 1
            continue
        batch.append(pair)
        if len(batch) >= BATCH_SIZE:
            c, d, f = flush(batch)
            total_created += c
            total_dup += d
            total_failed += f
            sys.stderr.write(
                f"[push] lote: created={c} dup={d} failed={f} "
                f"(acumulado created={total_created} dup={total_dup} failed={total_failed})\n"
            )
            batch = []

    c, d, f = flush(batch)
    total_created += c
    total_dup += d
    total_failed += f

    elapsed = time.time() - t0
    sys.stderr.write(
        f"\n[resumo] created={total_created} duplicate_409={total_dup} failed={total_failed} "
        f"skipped_sem_doc_id={total_skipped} tempo={elapsed:.1f}s indice={INDEX}\n"
    )
    if total_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
