#!/usr/bin/env bash
set -euo pipefail

# Carga imediata do prodesp-rolemembers-daily: roda o worker e manda o
# NDJSON DIRETO pro Elasticsearch via prodesp_rolemembers_push.py, sem
# reiniciar o Logstash e sem esperar o schedule. Serve pra validar worker +
# bootstrap + mapping ANTES do restart que liga o pipeline (e pra rodar um
# snapshot extra quando quiser comparar com o prodesp-directory).
#
# Variaveis do worker podem ser passadas na frente (valem sobre o .env), ex.:
#   PRODESP_ROLEMEMBERS_ROLES_MAX_COUNT=5 ./run_load_now.sh
#   PRODESP_ROLEMEMBERS_ROLES_FILTER="sysadmin,Acesso API" ./run_load_now.sh
#
# O NDJSON e gravado em arquivo ANTES do push: se o push falhar, reenvie:
#   python3 prodesp_rolemembers_push.py < /tmp/prodesp_rolemembers_load_<stamp>.ndjson
#
# Uso (le o .env sozinho, nada de export):
#   sudo -u logstash ./run_load_now.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Le o .env sozinho (credenciais ja preenchidas, nada de export). Variavel ja
# definida na linha de comando tem prioridade sobre o .env (util pra teste).
ENV_FILE="${ENV_FILE:-/etc/logstash/envio_prodesp_rolemembers.env}"
[ -f "$ENV_FILE" ] || ENV_FILE="$SCRIPT_DIR/envio_prodesp_rolemembers.env"
load_env() {
  [ -r "$ENV_FILE" ] || { echo "ERRO: nao consigo ler $ENV_FILE (rode como root ou logstash)" >&2; exit 1; }
  local saved
  saved=$(export -p | grep -E '^declare -x (PRODESP_ROLEMEMBERS_|ELASTIC_|KIBANA_)' | sed 's/^declare -x/declare -gx/' || true)
  set -a; . "$ENV_FILE"; set +a
  eval "$saved"
}
load_env

WORKER="/etc/logstash/pipelines/prodesp_rolemembers_worker.py"
[ -f "$WORKER" ] || WORKER="$SCRIPT_DIR/prodesp_rolemembers_worker.py"
PUSH_SCRIPT="$SCRIPT_DIR/prodesp_rolemembers_push.py"
[ -f "$PUSH_SCRIPT" ] || { echo "ERRO: $PUSH_SCRIPT nao encontrado (precisa estar ao lado deste script)." >&2; exit 1; }

: "${ELASTIC_URL:?defina ELASTIC_URL (source no .env antes de rodar)}"
: "${ELASTIC_USER:?defina ELASTIC_USER}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"
: "${PRODESP_ROLEMEMBERS_CLIENT_ID:?defina PRODESP_ROLEMEMBERS_CLIENT_ID}"
: "${PRODESP_ROLEMEMBERS_CLIENT_SECRET:?defina PRODESP_ROLEMEMBERS_CLIENT_SECRET}"
DATA_STREAM="${PRODESP_ROLEMEMBERS_DATA_STREAM:-logs-cyberark.prodesprolemembers-default}"

STAMP=$(date +%Y%m%d_%H%M%S)
TMPD="${TMPDIR:-/tmp}"
NDJSON="${TMPD}/prodesp_rolemembers_load_${STAMP}.ndjson"
WORKER_LOG="${TMPD}/prodesp_rolemembers_load_${STAMP}.worker.log"
PUSH_LOG="${TMPD}/prodesp_rolemembers_load_${STAMP}.push.log"

echo "== 1/3: worker (${WORKER}) — log em ${WORKER_LOG} =="
/usr/bin/python3 "$WORKER" 2>"$WORKER_LOG" >"$NDJSON"
grep -E '\[fim\]|falha nao recuperada' "$WORKER_LOG" || tail -5 "$WORKER_LOG"
echo "-- linhas no NDJSON (docs + heartbeat): $(wc -l < "$NDJSON")"

echo
echo "== 2/3: push pro Elastic (${DATA_STREAM}) — log em ${PUSH_LOG} =="
/usr/bin/python3 "$PUSH_SCRIPT" <"$NDJSON" 2>"$PUSH_LOG" || { tail -20 "$PUSH_LOG"; exit 1; }
tail -1 "$PUSH_LOG"

echo
echo "== 3/3: conferencia no Elastic (snapshot de hoje) =="
SNAP=$(TZ=America/Sao_Paulo date +%Y-%m-%d)
curl -s -u "${ELASTIC_USER}:${ELASTIC_PASSWORD}" "${ELASTIC_URL}/${DATA_STREAM}/_search" \
  -H 'Content-Type: application/json' \
  -d "{\"size\":0,\"track_total_hits\":true,\"query\":{\"term\":{\"snapshot_date\":\"${SNAP}\"}},\"aggs\":{\"tipo\":{\"terms\":{\"field\":\"doc_type\"}},\"users\":{\"filter\":{\"term\":{\"doc_type\":\"member\"}},\"aggs\":{\"distintos\":{\"cardinality\":{\"field\":\"Username\"}}}},\"status\":{\"filter\":{\"term\":{\"doc_type\":\"role_summary\"}},\"aggs\":{\"s\":{\"terms\":{\"field\":\"status\"}}}}}}" \
  | python3 -c 'import sys,json; r=json.load(sys.stdin); a=r["aggregations"]; print("   snapshot_date='"${SNAP}"' total=",r["hits"]["total"]["value"]); print("   por doc_type:",[(b["key"],b["doc_count"]) for b in a["tipo"]["buckets"]]); print("   usuarios distintos:",a["users"]["distintos"]["value"]); print("   role_summary por status (todas as execucoes do dia):",[(b["key"],b["doc_count"]) for b in a["status"]["s"]["buckets"]])'
echo
echo "Arquivos: ${NDJSON} | ${WORKER_LOG} | ${PUSH_LOG}"
