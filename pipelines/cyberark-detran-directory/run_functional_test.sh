#!/usr/bin/env bash
set -euo pipefail

# Teste funcional imediato do cyberark-detran-directory: roda o worker e
# manda o NDJSON DIRETO pro Elasticsearch via functional_push.py, sem
# passar pelo exec+schedule do Logstash. Existe pra dar certeza de que o
# processo funciona (auth, join, escrita no Elastic) sem depender do
# horario noturno nem reiniciar o Logstash -- ver DEPLOY.md, secao
# "Teste funcional imediato".
#
# Pre-requisito: bootstrap_cyberark_detran.sh ja rodado (ILM+template+
# data stream existem) -- ver DEPLOY.md passo 4. Sem isso os docs falham
# na escrita (create em indice sem template = erro de mapping/roteamento).
#
# Uso:
#   set -a; source /etc/logstash/envio_cyberark_detran.env; set +a
#   ./run_functional_test.sh                                            # carga completa (~20-25min)
#   DETRAN_USERS_PAGE_SIZE=200 DETRAN_USERS_MAX_PAGES=1 DETRAN_ROLES_MAX_COUNT=5 ./run_functional_test.sh   # smoke rapido (poucos minutos)
#   (sem limitar DETRAN_USERS_PAGE_SIZE, o default de producao e 50000 --
#   DETRAN_USERS_MAX_PAGES=1 sozinho NAO deixa o smoke pequeno, so limita
#   a 1 pagina de ate 50000 linhas reais)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

WORKER="/etc/logstash/pipelines/cyberark_detran_worker.py"
[ -f "$WORKER" ] || WORKER="$SCRIPT_DIR/cyberark_detran_worker.py"

PUSH_SCRIPT="$SCRIPT_DIR/functional_push.py"
if [ ! -f "$PUSH_SCRIPT" ]; then
    echo "ERRO: $PUSH_SCRIPT nao encontrado." >&2
    echo "functional_push.py precisa estar no MESMO diretorio que este script (${SCRIPT_DIR})." >&2
    exit 1
fi

: "${ELASTIC_URL:?defina ELASTIC_URL (source no .env antes de rodar)}"
: "${ELASTIC_USER:?defina ELASTIC_USER}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"
: "${DETRAN_DATA_STREAM:?defina DETRAN_DATA_STREAM}"

STAMP=$(date +%Y%m%d_%H%M%S)
NDJSON="/tmp/cyberark_detran_functional_${STAMP}.ndjson"
WORKER_LOG="/tmp/cyberark_detran_functional_${STAMP}.worker.log"
PUSH_LOG="/tmp/cyberark_detran_functional_${STAMP}.push.log"

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
echo "-- contagem atual no Elastic (${DETRAN_DATA_STREAM}):"
curl -s -u "${ELASTIC_USER}:${ELASTIC_PASSWORD}" "${ELASTIC_URL}/${DETRAN_DATA_STREAM}/_count"
echo
echo
echo "Arquivos desta rodada:"
echo "  ndjson: $NDJSON"
echo "  worker log: $WORKER_LOG"
echo "  push log: $PUSH_LOG"
