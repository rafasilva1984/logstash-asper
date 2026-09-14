#!/usr/bin/env bash
set -euo pipefail

# Teste funcional imediato do cyberark-apiprodesp-directory: roda o worker
# e manda o NDJSON DIRETO pro Elasticsearch via functional_push.py, sem
# passar pelo exec+schedule do Logstash. Existe pra dar certeza de que o
# processo funciona (auth, join, escrita no Elastic) sem depender do
# horario noturno nem reiniciar o Logstash -- mesmo padrao do
# cyberark-detran-directory, ver DEPLOY.md la, secao "Teste funcional
# imediato".
#
# Pre-requisito: bootstrap_cyberark_apiprodesp.sh ja rodado (ILM+template+
# data stream existem) -- sem isso os docs falham na escrita (create em
# indice sem template = erro de mapping/roteamento).
#
# Uso:
#   set -a; source /etc/logstash/envio_cyberark_apiprodesp.env; set +a
#   ./run_functional_test.sh                                            # carga completa (rapida, ~7k users)
#   APIPRODESP_USERS_PAGE_SIZE=200 APIPRODESP_USERS_MAX_PAGES=1 APIPRODESP_ROLES_MAX_COUNT=5 ./run_functional_test.sh   # smoke ainda mais rapido

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

WORKER="/etc/logstash/pipelines/cyberark_apiprodesp_worker.py"
[ -f "$WORKER" ] || WORKER="$SCRIPT_DIR/cyberark_apiprodesp_worker.py"

PUSH_SCRIPT="$SCRIPT_DIR/functional_push.py"
if [ ! -f "$PUSH_SCRIPT" ]; then
    echo "ERRO: $PUSH_SCRIPT nao encontrado." >&2
    echo "functional_push.py precisa estar no MESMO diretorio que este script (${SCRIPT_DIR})." >&2
    exit 1
fi

: "${ELASTIC_URL:?defina ELASTIC_URL (source no .env antes de rodar)}"
: "${ELASTIC_USER:?defina ELASTIC_USER}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"
: "${APIPRODESP_DATA_STREAM:?defina APIPRODESP_DATA_STREAM}"

STAMP=$(date +%Y%m%d_%H%M%S)
NDJSON="/tmp/cyberark_apiprodesp_functional_${STAMP}.ndjson"
WORKER_LOG="/tmp/cyberark_apiprodesp_functional_${STAMP}.worker.log"
PUSH_LOG="/tmp/cyberark_apiprodesp_functional_${STAMP}.push.log"

echo "== 1/3: rodando worker (${WORKER}) — log em ${WORKER_LOG} =="
python3 "$WORKER" 2>"$WORKER_LOG" \
  | tee "$NDJSON" \
  | python3 "$PUSH_SCRIPT" 2>"$PUSH_LOG"

echo
echo "== 2/3: resumo do push =="
tail -5 "$PUSH_LOG"

echo
echo "== 3/3: checklist rapido =="
echo "-- linhas no NDJSON (dado + heartbeat): $(wc -l < "$NDJSON")"
echo "-- amostra (confirmar last_login_ms preenchido; role_id/role_name quando aplicavel):"
head -3 "$NDJSON"
echo
echo "-- contagem atual no Elastic (${APIPRODESP_DATA_STREAM}):"
curl -s -u "${ELASTIC_USER}:${ELASTIC_PASSWORD}" "${ELASTIC_URL}/${APIPRODESP_DATA_STREAM}/_count"
echo
echo
echo "Arquivos desta rodada:"
echo "  ndjson: $NDJSON"
echo "  worker log: $WORKER_LOG"
echo "  push log: $PUSH_LOG"
