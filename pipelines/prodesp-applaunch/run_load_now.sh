#!/usr/bin/env bash
set -euo pipefail

# Carga imediata do prodesp-applaunch-realtime: roda o worker e manda o
# NDJSON DIRETO pro Elasticsearch via prodesp_applaunch_push.py, sem
# reiniciar o Logstash. Serve pra validar worker + bootstrap + mapping
# ANTES do restart que liga o pipeline.
#
# Sem argumentos: modo checkpoint de verdade (sem checkpoint = ultima
# INITIAL_LOOKBACK_HOURS; LE e GRAVA o checkpoint real). O pipeline, quando
# ligado, continua dali (checkpoint - overlap), sem buraco e sem duplicar.
# Com --since/--until: modo manual, janela explicita, NAO toca no checkpoint.
#
# O NDJSON e gravado em arquivo ANTES do push: se o push falhar depois que
# o worker ja avancou o checkpoint, reenvie o mesmo arquivo:
#   python3 prodesp_applaunch_push.py < /tmp/prodesp_applaunch_load_<stamp>.ndjson
#
# Pre-requisitos: bootstrap_prodesp_applaunch.sh ja rodado; diretorio do
# checkpoint criado e do usuario logstash (rodar este script como logstash).
#
# Uso:
#   set -a; source /etc/logstash/envio_prodesp_applaunch.env; set +a
#   sudo -E -u logstash ./run_load_now.sh
#   sudo -E -u logstash ./run_load_now.sh --since 2026-10-21T00:00:00-03:00 --until 2026-10-21T01:00:00-03:00

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

WORKER="/etc/logstash/pipelines/prodesp_applaunch_worker.py"
[ -f "$WORKER" ] || WORKER="$SCRIPT_DIR/prodesp_applaunch_worker.py"
PUSH_SCRIPT="$SCRIPT_DIR/prodesp_applaunch_push.py"
[ -f "$PUSH_SCRIPT" ] || { echo "ERRO: $PUSH_SCRIPT nao encontrado (precisa estar ao lado deste script)." >&2; exit 1; }

: "${ELASTIC_URL:?defina ELASTIC_URL (source no .env antes de rodar)}"
: "${ELASTIC_USER:?defina ELASTIC_USER}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"
: "${PRODESP_APPLAUNCH_CLIENT_ID:?defina PRODESP_APPLAUNCH_CLIENT_ID}"
: "${PRODESP_APPLAUNCH_CLIENT_SECRET:?defina PRODESP_APPLAUNCH_CLIENT_SECRET}"
DATA_STREAM="${PRODESP_APPLAUNCH_DATA_STREAM:-logs-cyberark.prodespapplaunch-default}"

STAMP=$(date +%Y%m%d_%H%M%S)
NDJSON="/tmp/prodesp_applaunch_load_${STAMP}.ndjson"
WORKER_LOG="/tmp/prodesp_applaunch_load_${STAMP}.worker.log"
PUSH_LOG="/tmp/prodesp_applaunch_load_${STAMP}.push.log"

echo "== 1/3: worker (${WORKER} $*) — log em ${WORKER_LOG} =="
/usr/bin/python3 "$WORKER" "$@" 2>"$WORKER_LOG" >"$NDJSON"
tail -5 "$WORKER_LOG"
echo "-- linhas no NDJSON (docs + heartbeat): $(wc -l < "$NDJSON")"

echo
echo "== 2/3: push pro Elastic (${DATA_STREAM}) — log em ${PUSH_LOG} =="
python3 "$PUSH_SCRIPT" <"$NDJSON" 2>"$PUSH_LOG" || { tail -20 "$PUSH_LOG"; exit 1; }
tail -1 "$PUSH_LOG"

echo
echo "== 3/3: conferencia no Elastic =="
curl -s -u "${ELASTIC_USER}:${ELASTIC_PASSWORD}" "${ELASTIC_URL}/${DATA_STREAM}/_search" \
  -H 'Content-Type: application/json' \
  -d '{"size":0,"track_total_hits":true,"aggs":{"app":{"terms":{"field":"ApplicationName","size":5}},"selo":{"terms":{"field":"Selo_level"}},"sem_user":{"filter":{"term":{"user_found":false}}},"min":{"min":{"field":"@timestamp"}},"max":{"max":{"field":"@timestamp"}}}}' \
  | python3 -c 'import sys,json; r=json.load(sys.stdin); a=r["aggregations"]; print("   total=",r["hits"]["total"]["value"],"| user_found=false:",a["sem_user"]["doc_count"]); print("   top apps:",[(b["key"],b["doc_count"]) for b in a["app"]["buckets"]]); print("   Selo_level:",[(b["key"],b["doc_count"]) for b in a["selo"]["buckets"]]); print("   @timestamp min/max:",a["min"].get("value_as_string"),"/",a["max"].get("value_as_string"))'
echo "-- checkpoint: $(cat "${PRODESP_APPLAUNCH_CHECKPOINT_FILE:-/var/lib/logstash/prodesp-applaunch/checkpoint}" 2>/dev/null || echo '(nao gravado — modo manual ou falha)')"
echo
echo "Arquivos: ${NDJSON} | ${WORKER_LOG} | ${PUSH_LOG}"
