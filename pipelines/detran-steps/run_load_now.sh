#!/usr/bin/env bash
set -euo pipefail

# Carga imediata do detran-steps-daily: roda o worker e manda o NDJSON DIRETO
# pro Elasticsearch via detran_steps_push.py, sem esperar as 07:00 e sem
# reiniciar o Logstash.
#
# Sem argumentos: modo AGENDADO de verdade (ultimas 24h, LE e GRAVA o
# checkpoint real). A execucao das 07:00 seguinte continua dali (janela =
# checkpoint - overlap -> agora), sem buraco e sem duplicar.
# Com --since/--until: modo manual, janela explicita, NAO toca no checkpoint.
#
# O NDJSON e gravado em arquivo ANTES do push: se o push falhar depois que
# o worker ja avancou o checkpoint, reenvie o mesmo arquivo:
#   python3 detran_steps_push.py < /tmp/detran_steps_load_<stamp>.ndjson
#
# Pre-requisitos: bootstrap_detran_steps.sh ja rodado; diretorio do checkpoint
# criado e do usuario logstash (rodar este script como logstash).
#
# Uso:
#   set -a; source /etc/logstash/envio_detran_steps.env; set +a
#   sudo -E -u logstash ./run_load_now.sh
#   sudo -E -u logstash ./run_load_now.sh --since 2026-10-01T00:00:00-03:00 --until 2026-10-02T00:00:00-03:00

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

WORKER="/etc/logstash/pipelines/detran_steps_worker.py"
[ -f "$WORKER" ] || WORKER="$SCRIPT_DIR/detran_steps_worker.py"
PUSH_SCRIPT="$SCRIPT_DIR/detran_steps_push.py"
[ -f "$PUSH_SCRIPT" ] || { echo "ERRO: $PUSH_SCRIPT nao encontrado (precisa estar ao lado deste script)." >&2; exit 1; }

: "${ELASTIC_URL:?defina ELASTIC_URL (source no .env antes de rodar)}"
: "${ELASTIC_USER:?defina ELASTIC_USER}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"
: "${DETRAN_STEPS_CLIENT_ID:?defina DETRAN_STEPS_CLIENT_ID}"
: "${DETRAN_STEPS_CLIENT_SECRET:?defina DETRAN_STEPS_CLIENT_SECRET}"
DATA_STREAM="${DETRAN_STEPS_DATA_STREAM:-logs-detran.steps-default}"

STAMP=$(date +%Y%m%d_%H%M%S)
NDJSON="/tmp/detran_steps_load_${STAMP}.ndjson"
WORKER_LOG="/tmp/detran_steps_load_${STAMP}.worker.log"
PUSH_LOG="/tmp/detran_steps_load_${STAMP}.push.log"

echo "== 1/3: worker (${WORKER} $*) — log em ${WORKER_LOG} =="
/usr/bin/python3 "$WORKER" "$@" 2>"$WORKER_LOG" >"$NDJSON"
tail -3 "$WORKER_LOG"
echo "-- linhas no NDJSON (docs + heartbeat): $(wc -l < "$NDJSON")"

echo
echo "== 2/3: push pro Elastic (${DATA_STREAM}) — log em ${PUSH_LOG} =="
python3 "$PUSH_SCRIPT" <"$NDJSON" 2>"$PUSH_LOG" || { tail -20 "$PUSH_LOG"; exit 1; }
tail -1 "$PUSH_LOG"

echo
echo "== 3/3: conferencia no Elastic =="
echo "-- count: $(curl -s -u "${ELASTIC_USER}:${ELASTIC_PASSWORD}" "${ELASTIC_URL}/${DATA_STREAM}/_count")"
echo "-- docs por extraction_status:"
curl -s -u "${ELASTIC_USER}:${ELASTIC_PASSWORD}" "${ELASTIC_URL}/${DATA_STREAM}/_search" \
  -H 'Content-Type: application/json' \
  -d '{"size":0,"aggs":{"st":{"terms":{"field":"extraction_status"}},"min":{"min":{"field":"@timestamp"}},"max":{"max":{"field":"@timestamp"}}}}' \
  | python3 -c 'import sys,json; a=json.load(sys.stdin)["aggregations"]; [print("  ",b["key"],b["doc_count"]) for b in a["st"]["buckets"]]; print("   @timestamp min/max:",a["min"].get("value_as_string"),"/",a["max"].get("value_as_string"))'
echo "-- checkpoint: $(cat "${DETRAN_STEPS_CHECKPOINT_FILE:-/var/lib/logstash/detran-steps/checkpoint}" 2>/dev/null || echo '(nao gravado — modo manual ou falha)')"
echo
echo "Arquivos: ${NDJSON} | ${WORKER_LOG} | ${PUSH_LOG}"
