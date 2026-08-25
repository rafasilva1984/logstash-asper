#!/usr/bin/env bash
set -euo pipefail

# Teste funcional imediato do cyberark-prodesp-directory: roda o worker e
# manda o NDJSON DIRETO pro Elasticsearch via functional_push.py, sem
# passar pelo exec+schedule do Logstash. Existe pra dar certeza de que o
# processo funciona (auth, join, escrita no Elastic) sem depender do
# horario noturno nem reiniciar o Logstash -- ver DEPLOY.md, secao
# "Teste funcional imediato".
#
# Pre-requisito: bootstrap_cyberark_prodesp.sh ja rodado (ILM+template+
# data stream existem) -- ver DEPLOY.md passo 4. Sem isso os docs falham
# na escrita (create em indice sem template = erro de mapping/roteamento).
#
# Uso:
#   set -a; source /etc/logstash/envio_cyberark_prodesp.env; set +a
#   ./run_functional_test.sh                                            # carga completa (~20-25min)
#   PRODESP_USERS_MAX_PAGES=1 PRODESP_ROLES_MAX_COUNT=5 ./run_functional_test.sh   # smoke rapido (poucos minutos)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

WORKER="/etc/logstash/pipelines/cyberark_prodesp_worker.py"
[ -f "$WORKER" ] || WORKER="$SCRIPT_DIR/cyberark_prodesp_worker.py"

: "${ELASTIC_URL:?defina ELASTIC_URL (source no .env antes de rodar)}"
: "${ELASTIC_USER:?defina ELASTIC_USER}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"
: "${PRODESP_DATA_STREAM:?defina PRODESP_DATA_STREAM}"

STAMP=$(date +%Y%m%d_%H%M%S)
NDJSON="/tmp/cyberark_prodesp_functional_${STAMP}.ndjson"
WORKER_LOG="/tmp/cyberark_prodesp_functional_${STAMP}.worker.log"
PUSH_LOG="/tmp/cyberark_prodesp_functional_${STAMP}.push.log"

echo "== 1/3: rodando worker (${WORKER}) — log em ${WORKER_LOG} =="
python3 "$WORKER" 2>"$WORKER_LOG" \
  | tee "$NDJSON" \
  | python3 "$SCRIPT_DIR/functional_push.py" 2>"$PUSH_LOG"

echo
echo "== 2/3: resumo do push =="
tail -5 "$PUSH_LOG"

echo
echo "== 3/3: checklist rapido =="
echo "-- linhas no NDJSON (dado + heartbeat): $(wc -l < "$NDJSON")"
echo "-- amostra (confirmar last_login_ms preenchido; role_id/role_name quando aplicavel):"
head -3 "$NDJSON"
echo
echo "-- contagem atual no Elastic (${PRODESP_DATA_STREAM}):"
curl -s -u "${ELASTIC_USER}:${ELASTIC_PASSWORD}" "${ELASTIC_URL}/${PRODESP_DATA_STREAM}/_count"
echo
echo
echo "Arquivos desta rodada:"
echo "  ndjson: $NDJSON"
echo "  worker log: $WORKER_LOG"
echo "  push log: $PUSH_LOG"
