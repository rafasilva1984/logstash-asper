#!/usr/bin/env bash
# Bootstrap do ILM policy + index template + data stream do pipeline
# sgp-steps-daily (fonte Alero, tenant SGP). Idempotente: reexecutar
# atualiza policy e template; o data stream so e criado se nao existir.
#
# Retencao (pedido do cliente): 30 dias hot -> 300 dias frozen -> DELETE.
# E a primeira policy da plataforma COM fase de delete (as demais retem
# para sempre). Para desligar o delete: SGP_STEPS_ILM_DELETE=false e
# reexecutar este script (vale enquanto nenhum indice chegou a fase).
#
# min_age das fases conta a partir do ROLLOVER do indice backing. Com
# rollover a cada SGP_STEPS_ILM_ROLLOVER_MAX_AGE (default 7d), cada dado
# fica hot entre 30 e 37 dias e e apagado entre 330 e 337 dias.
#
# Uso:
#   set -a; source /etc/logstash/envio_sgp_steps.env; set +a
#   ./bootstrap_sgp_steps.sh
#
# Variaveis:
#   ELASTIC_URL, ELASTIC_USER, ELASTIC_PASSWORD     (obrigatorias)
#   ELASTIC_SNAPSHOT_REPOSITORY      (default: found-snapshots)
#   SGP_STEPS_DATA_STREAM            (default: logs-sgp.steps-default)
#   SGP_STEPS_ILM_ROLLOVER_MAX_AGE   (default: 7d)
#   SGP_STEPS_ILM_HOT_DAYS           (default: 30)
#   SGP_STEPS_ILM_FROZEN_DAYS        (default: 300)
#   SGP_STEPS_ILM_DELETE             (default: true)
#   KIBANA_URL, KIBANA_DATA_VIEW_NAME   (opcionais, criam o data view)

set -euo pipefail

: "${ELASTIC_URL:?defina ELASTIC_URL}"
: "${ELASTIC_USER:?defina ELASTIC_USER}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"

SNAPSHOT_REPO="${ELASTIC_SNAPSHOT_REPOSITORY:-found-snapshots}"
DATA_STREAM="${SGP_STEPS_DATA_STREAM:-logs-sgp.steps-default}"
ILM_POLICY="logs-sgp.steps-ilm"
TEMPLATE_NAME="logs-sgp.steps-template"
ROLLOVER_MAX_AGE="${SGP_STEPS_ILM_ROLLOVER_MAX_AGE:-7d}"
HOT_DAYS="${SGP_STEPS_ILM_HOT_DAYS:-30}"
FROZEN_DAYS="${SGP_STEPS_ILM_FROZEN_DAYS:-300}"
DELETE_ENABLED="${SGP_STEPS_ILM_DELETE:-true}"
DELETE_DAYS=$((HOT_DAYS + FROZEN_DAYS))

AUTH=(-u "${ELASTIC_USER}:${ELASTIC_PASSWORD}")

# PUT que falha alto: curl -sS sozinho nao da erro em HTTP 4xx/5xx.
put_json() {
  local url="$1" body code
  body=$(cat)
  code=$(curl -sS "${AUTH[@]}" -o /tmp/bootstrap_sgp_steps.out -w '%{http_code}' \
    -X PUT "$url" -H 'Content-Type: application/json' -d "$body")
  cat /tmp/bootstrap_sgp_steps.out; echo
  if [ "$code" != "200" ]; then
    echo "ERRO: HTTP $code em PUT $url" >&2
    exit 1
  fi
}

echo ">> verificando repositorio de snapshot '${SNAPSHOT_REPO}'..."
code=$(curl -sS "${AUTH[@]}" -o /dev/null -w '%{http_code}' "${ELASTIC_URL}/_snapshot/${SNAPSHOT_REPO}")
if [ "$code" != "200" ]; then
  echo "ERRO: repositorio de snapshot '${SNAPSHOT_REPO}' nao encontrado (http $code)." >&2
  exit 1
fi

DELETE_PHASE=""
if [ "$DELETE_ENABLED" = "true" ]; then
  DELETE_PHASE=",
      \"delete\": {
        \"min_age\": \"${DELETE_DAYS}d\",
        \"actions\": { \"delete\": {} }
      }"
fi

echo ">> criando/atualizando ILM policy '${ILM_POLICY}' (hot ${HOT_DAYS}d -> frozen ${FROZEN_DAYS}d -> delete=${DELETE_ENABLED})..."
put_json "${ELASTIC_URL}/_ilm/policy/${ILM_POLICY}" <<EOF
{
  "policy": {
    "phases": {
      "hot": {
        "min_age": "0ms",
        "actions": {
          "rollover": {
            "max_age": "${ROLLOVER_MAX_AGE}",
            "max_primary_shard_size": "50gb"
          },
          "set_priority": { "priority": 100 }
        }
      },
      "frozen": {
        "min_age": "${HOT_DAYS}d",
        "actions": {
          "searchable_snapshot": {
            "snapshot_repository": "${SNAPSHOT_REPO}"
          }
        }
      }${DELETE_PHASE}
    }
  }
}
EOF

echo ">> criando/atualizando index template '${TEMPLATE_NAME}'..."
put_json "${ELASTIC_URL}/_index_template/${TEMPLATE_NAME}" <<EOF
{
  "index_patterns": ["logs-sgp.steps-*"],
  "data_stream": {},
  "priority": 200,
  "template": {
    "settings": {
      "index.lifecycle.name": "${ILM_POLICY}"
    },
    "mappings": {
      "dynamic": false,
      "properties": {
        "@timestamp":                   { "type": "date" },
        "data_stream": {
          "properties": {
            "dataset":   { "type": "constant_keyword", "value": "sgp.steps" },
            "namespace": { "type": "constant_keyword", "value": "default" },
            "type":      { "type": "constant_keyword", "value": "logs" }
          }
        },
        "recording_id":                 { "type": "keyword" },
        "user_id":                      { "type": "keyword" },
        "user_fullName":                { "type": "keyword" },
        "user_username":                { "type": "keyword" },
        "application_id":               { "type": "keyword" },
        "application_name":             { "type": "keyword" },
        "recording_startDatetime":      { "type": "date", "format": "epoch_millis" },
        "recording_endDatetime":        { "type": "date", "format": "epoch_millis" },
        "recording_durationSeconds":    { "type": "long" },
        "recording_stepsCount":         { "type": "integer" },
        "recording_flag":               { "type": "keyword" },
        "recording_tabIds":             { "type": "keyword" },
        "extraction_status":            { "type": "keyword" },
        "steps_recebidos":              { "type": "integer" },
        "tempo_processamento_sessao_s": { "type": "float" },
        "event_uid":                    { "type": "keyword" },
        "step_index":                   { "type": "integer" },
        "step_id":                      { "type": "keyword" },
        "step_datetime":                { "type": "date", "format": "epoch_millis" },
        "step_extensionEventId":        { "type": "keyword" },
        "step_flag":                    { "type": "keyword" },
        "step_tabId":                   { "type": "keyword" },
        "step_type":                    { "type": "keyword" },
        "step_url":                     { "type": "keyword", "ignore_above": 2048 },
        "step_windowTitle":             { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 512 } } },
        "step_windowHeight":            { "type": "integer" },
        "step_windowWidth":             { "type": "integer" },
        "step_clientTimeZone":          { "type": "keyword" },
        "step_elementXPath":            { "type": "keyword", "ignore_above": 2048 },
        "step_pressedKey":              { "type": "keyword" },
        "step_cursorX":                 { "type": "integer" },
        "step_cursorY":                 { "type": "integer" },
        "step_elementTag":              { "type": "keyword" },
        "step_elementText":             { "type": "text" },
        "step_elementValue":            { "type": "text" },
        "step_elementOldValue":         { "type": "text" },
        "value_old":                    { "type": "text" },
        "value_new":                    { "type": "text" },
        "is_value_change":              { "type": "boolean" },
        "is_mouse_click":               { "type": "boolean" },
        "is_navigated_to":              { "type": "boolean" },
        "is_clipboard_paste":           { "type": "boolean" }
      }
    }
  }
}
EOF

echo ">> criando data stream '${DATA_STREAM}' (se ainda nao existir)..."
code=$(curl -sS "${AUTH[@]}" -o /dev/null -w '%{http_code}' "${ELASTIC_URL}/_data_stream/${DATA_STREAM}")
if [ "$code" = "200" ]; then
  echo "   ja existe — mantido."
else
  code=$(curl -sS "${AUTH[@]}" -o /tmp/bootstrap_sgp_steps.out -w '%{http_code}' -X PUT "${ELASTIC_URL}/_data_stream/${DATA_STREAM}")
  cat /tmp/bootstrap_sgp_steps.out; echo
  [ "$code" = "200" ] || { echo "ERRO: HTTP $code criando data stream" >&2; exit 1; }
fi

echo ">> conferindo: indice backing usa a policy?"
curl -sS "${AUTH[@]}" "${ELASTIC_URL}/_data_stream/${DATA_STREAM}" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin)["data_streams"][0]; print("   template=",d["template"],"| ilm_policy=",d.get("ilm_policy"),"| backing=",[i["index_name"] for i in d["indices"]])'

if [ -n "${KIBANA_URL:-}" ]; then
  echo ">> criando data view no Kibana..."
  curl -sS "${AUTH[@]}" -X POST "${KIBANA_URL}/api/data_views/data_view" \
    -H 'Content-Type: application/json' -H 'kbn-xsrf: true' \
    -d "{\"data_view\":{\"title\":\"${DATA_STREAM}\",\"name\":\"${KIBANA_DATA_VIEW_NAME:-SGP Steps}\",\"timeFieldName\":\"@timestamp\"}}"
  echo
fi

echo ">> bootstrap concluido."
