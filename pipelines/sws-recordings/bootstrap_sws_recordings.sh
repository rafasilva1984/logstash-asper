#!/usr/bin/env bash
# Bootstrap do ILM policy + index template + data stream do pipeline
# sws-recordings (fonte Alero). Idempotente para criar/atualizar policy e
# template. O DROP do data stream antigo (mesmo nome, schema antigo) so
# roda se --drop-existing for passado explicitamente — e IRREVERSIVEL.
#
# Uso:
#   source envio_sws_recordings.env
#   export ELASTIC_USERNAME="$ELASTIC_USER"   # bootstrap usa ELASTIC_USERNAME
#   ./bootstrap_sws_recordings.sh                  # cria/atualiza ILM+template+data stream
#   ./bootstrap_sws_recordings.sh --drop-existing   # ANTES apaga o data stream antigo
#
# Variaveis usadas (via ambiente, tipicamente vindas do .env do pipeline):
#   ELASTIC_URL, ELASTIC_USERNAME, ELASTIC_PASSWORD   (obrigatorias)
#   ELASTIC_SNAPSHOT_REPOSITORY   (default: found-snapshots)
#   SWS_RECORDINGS_DATA_STREAM    (default: logs-sws.recordings-default)
#   SWS_ILM_HOT_MAX_AGE           (default: 15d)
#   SWS_ILM_HOT_MAX_SHARD_SIZE    (default: 50gb)
#   KIBANA_URL, KIBANA_DATA_VIEW_NAME   (opcionais, criam o data view)

set -euo pipefail

: "${ELASTIC_URL:?defina ELASTIC_URL}"
: "${ELASTIC_USERNAME:?defina ELASTIC_USERNAME}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"

SNAPSHOT_REPO="${ELASTIC_SNAPSHOT_REPOSITORY:-found-snapshots}"
DATA_STREAM="${SWS_RECORDINGS_DATA_STREAM:-logs-sws.recordings-default}"
ILM_POLICY="logs-sws.recordings-ilm"
TEMPLATE_NAME="logs-sws.recordings-template"
HOT_MAX_AGE="${SWS_ILM_HOT_MAX_AGE:-15d}"
HOT_MAX_SHARD_SIZE="${SWS_ILM_HOT_MAX_SHARD_SIZE:-50gb}"

AUTH=(-u "${ELASTIC_USERNAME}:${ELASTIC_PASSWORD}")

echo ">> verificando repositorio de snapshot '${SNAPSHOT_REPO}'..."
code=$(curl -sS "${AUTH[@]}" -o /dev/null -w '%{http_code}' "${ELASTIC_URL}/_snapshot/${SNAPSHOT_REPO}")
if [ "$code" != "200" ]; then
  echo "ERRO: repositorio de snapshot '${SNAPSHOT_REPO}' nao encontrado (http $code)."
  exit 1
fi

if [ "${1:-}" == "--drop-existing" ]; then
  echo ">> --drop-existing: apagando data stream '${DATA_STREAM}' (IRREVERSIVEL)..."
  curl -sS "${AUTH[@]}" -X DELETE "${ELASTIC_URL}/_data_stream/${DATA_STREAM}" || true
  echo
fi

echo ">> criando/atualizando ILM policy '${ILM_POLICY}'..."
curl -sS "${AUTH[@]}" -X PUT "${ELASTIC_URL}/_ilm/policy/${ILM_POLICY}" \
  -H 'Content-Type: application/json' -d @- <<EOF
{
  "policy": {
    "phases": {
      "hot": {
        "min_age": "0ms",
        "actions": {
          "rollover": {
            "max_age": "${HOT_MAX_AGE}",
            "max_primary_shard_size": "${HOT_MAX_SHARD_SIZE}"
          },
          "set_priority": { "priority": 100 }
        }
      },
      "frozen": {
        "min_age": "${HOT_MAX_AGE}",
        "actions": {
          "searchable_snapshot": {
            "snapshot_repository": "${SNAPSHOT_REPO}"
          }
        }
      }
    }
  }
}
EOF
echo

echo ">> criando/atualizando index template '${TEMPLATE_NAME}'..."
curl -sS "${AUTH[@]}" -X PUT "${ELASTIC_URL}/_index_template/${TEMPLATE_NAME}" \
  -H 'Content-Type: application/json' -d @- <<EOF
{
  "index_patterns": ["${DATA_STREAM}*", "logs-sws.recordings-*"],
  "data_stream": {},
  "priority": 200,
  "template": {
    "settings": {
      "index.lifecycle.name": "${ILM_POLICY}"
    },
    "mappings": {
      "dynamic": false,
      "properties": {
        "@timestamp":                { "type": "date" },
        "data_stream": {
          "properties": {
            "dataset":   { "type": "constant_keyword", "value": "sws.recordings" },
            "namespace": { "type": "constant_keyword", "value": "default" },
            "type":      { "type": "constant_keyword", "value": "logs" }
          }
        },
        "recording_id":              { "type": "keyword" },
        "user_id":                   { "type": "keyword" },
        "user_fullName":             { "type": "keyword" },
        "user_username":             { "type": "keyword" },
        "application_id":            { "type": "keyword" },
        "application_name":          { "type": "keyword" },
        "recording_startDatetime":   { "type": "date", "format": "epoch_millis" },
        "recording_endDatetime":     { "type": "date", "format": "epoch_millis" },
        "recording_durationSeconds": { "type": "long" },
        "recording_stepsCount":      { "type": "integer" },
        "recording_flag":            { "type": "keyword" },
        "recording_tabIds":          { "type": "keyword" },
        "extraction_status":         { "type": "keyword" },
        "extraction_error":          { "type": "text" },
        "steps_capturados":          { "type": "integer" },
        "step_index":                { "type": "integer" },
        "step_id":                   { "type": "keyword" },
        "step_datetime":             { "type": "date", "format": "epoch_millis" },
        "step_extensionEventId":     { "type": "keyword" },
        "step_flag":                 { "type": "keyword" },
        "step_tabId":                { "type": "keyword" },
        "step_type":                 { "type": "keyword" },
        "step_url":                  { "type": "keyword", "ignore_above": 2048 },
        "step_windowTitle":          { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 512 } } },
        "step_windowHeight":         { "type": "integer" },
        "step_windowWidth":          { "type": "integer" },
        "step_clientTimeZone":       { "type": "keyword" },
        "step_elementXPath":         { "type": "keyword", "ignore_above": 2048 },
        "step_pressedKey":           { "type": "keyword" },
        "step_cursorX":              { "type": "integer" },
        "step_cursorY":              { "type": "integer" },
        "step_elementTag":           { "type": "keyword" },
        "step_elementText":          { "type": "text" },
        "step_elementValue":         { "type": "text" },
        "step_elementOldValue":      { "type": "text" },
        "value_old":                 { "type": "text" },
        "value_new":                 { "type": "text" },
        "is_value_change":           { "type": "boolean" },
        "is_mouse_click":            { "type": "boolean" },
        "is_navigated_to":           { "type": "boolean" },
        "is_clipboard_paste":        { "type": "boolean" }
      }
    }
  }
}
EOF
echo

echo ">> criando data stream '${DATA_STREAM}' (se ainda nao existir)..."
curl -sS "${AUTH[@]}" -X PUT "${ELASTIC_URL}/_data_stream/${DATA_STREAM}"
echo

if [ -n "${KIBANA_URL:-}" ]; then
  echo ">> criando data view no Kibana..."
  curl -sS "${AUTH[@]}" -X POST "${KIBANA_URL}/api/data_views/data_view" \
    -H 'Content-Type: application/json' -H 'kbn-xsrf: true' \
    -d "{\"data_view\":{\"title\":\"${DATA_STREAM}\",\"name\":\"${KIBANA_DATA_VIEW_NAME:-SWS Recordings}\",\"timeFieldName\":\"@timestamp\"}}"
  echo
fi

echo ">> bootstrap concluido."
