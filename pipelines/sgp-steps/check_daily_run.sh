#!/usr/bin/env bash
# Validacao da execucao agendada (07:00) do sgp-steps-daily.
# Rodar no servidor DEPOIS das 07:00 (o worker leva alguns minutos).
#
# Uso:
#   set -a; source /etc/logstash/envio_sgp_steps.env; set +a
#   /mnt/asper/scripts/check_daily_run.sh            (ou ./check_daily_run.sh)
#
# Criterio de OK:
#   1. checkpoint ~07:00 de hoje (gravado em UTC -> ~10:00Z)
#   2. pipeline com in/out > 0 desde o ultimo restart
#   3. @timestamp mais recente no Elastic e de hoje (steps das ultimas 24h)
# 409 no log do Logstash e normal (overlap de 1h com a execucao anterior).
# Se o checkpoint nao mudou, o schedule nao disparou: ver o log (item 4),
# pipelines.yml e se SGP_STEPS_SCHEDULE esta entre aspas no .env.

set -uo pipefail

PIPELINE="sgp-steps-daily"
CHECKPOINT="${SGP_STEPS_CHECKPOINT_FILE:-/var/lib/logstash/sgp-steps/checkpoint}"
DATA_STREAM="${SGP_STEPS_DATA_STREAM:-logs-sgp.steps-default}"
: "${ELASTIC_URL:?source no .env antes}"; : "${ELASTIC_USER:?}"; : "${ELASTIC_PASSWORD:?}"

echo "== ${PIPELINE} =="
echo "-- 1. checkpoint: $(cat "$CHECKPOINT" 2>/dev/null || echo 'INEXISTENTE')  (esperado ~$(date -u -d "TZ=\"America/Sao_Paulo\" $(TZ=America/Sao_Paulo date +%F) 07:00" +%Y-%m-%dT%H:%MZ 2>/dev/null || echo '10:00Z de hoje'))"

echo -n "-- 2. eventos no pipeline: "
curl -s --max-time 10 "http://127.0.0.1:9600/_node/stats/pipelines/${PIPELINE}" \
  | python3 -c "import sys,json;d=json.load(sys.stdin)['pipelines']['${PIPELINE}']['events'];print('in=',d['in'],'filtered=',d['filtered'],'out=',d['out'])" \
  2>/dev/null || echo "API 9600 sem resposta ou pipeline nao carregado"

echo "-- 3. Elastic (${DATA_STREAM}):"
curl -s --max-time 30 -u "${ELASTIC_USER}:${ELASTIC_PASSWORD}" "${ELASTIC_URL}/${DATA_STREAM}/_search" \
  -H 'Content-Type: application/json' \
  -d '{"size":0,"track_total_hits":true,"aggs":{"max":{"max":{"field":"@timestamp"}},"ult24h":{"filter":{"range":{"@timestamp":{"gte":"now-24h"}}}},"st":{"terms":{"field":"extraction_status"}}}}' \
  | python3 -c 'import sys,json; r=json.load(sys.stdin); a=r["aggregations"]; print("   total=",r["hits"]["total"]["value"],"| ultimas 24h=",a["ult24h"]["doc_count"],"| @timestamp max=",a["max"].get("value_as_string")); [print("   ",b["key"],b["doc_count"]) for b in a["st"]["buckets"]]' \
  2>/dev/null || echo "   falha consultando o Elastic (credencial/URL/data stream?)"

echo "-- 4. log do Logstash (ultimas linhas do pipeline):"
grep -i "sgp_steps\|${PIPELINE}" /var/log/logstash/logstash-plain.log 2>/dev/null | tail -10 || echo "   (sem acesso ao log)"
