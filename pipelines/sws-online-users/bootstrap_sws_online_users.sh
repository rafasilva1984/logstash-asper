#!/usr/bin/env bash
# Bootstrap do ILM policy + index template + data stream do pipeline
# sws-online-users (fonte Alero). Idempotente para criar/atualizar policy e
# template. O DROP do data stream so roda se --drop-existing for passado
# explicitamente — e IRREVERSIVEL.
#
# Uso:
#   source envio_sws_online_users.env
#   export ELASTIC_USERNAME="$ELASTIC_USER"   # bootstrap usa ELASTIC_USERNAME
#   ./bootstrap_sws_online_users.sh                  # cria/atualiza ILM+template+data stream
#   ./bootstrap_sws_online_users.sh --drop-existing   # ANTES apaga o data stream existente
#
# Variaveis usadas (via ambiente, tipicamente vindas do .env do pipeline):
#   ELASTIC_URL, ELASTIC_USERNAME, ELASTIC_PASSWORD   (obrigatorias)
#   ELASTIC_SNAPSHOT_REPOSITORY   (default: found-snapshots)
#   SWS_ONLINE_USERS_DATA_STREAM  (default: logs-sws.online-users-default)
#   SWS_ONLINE_ILM_HOT_MAX_AGE          (default: 30d)
#   SWS_ONLINE_ILM_HOT_MAX_SHARD_SIZE   (default: 50gb)
#   KIBANA_URL, KIBANA_DATA_VIEW_NAME   (opcionais, criam o data view)

set -euo pipefail

: "${ELASTIC_URL:?defina ELASTIC_URL}"
: "${ELASTIC_USERNAME:?defina ELASTIC_USERNAME}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"

SNAPSHOT_REPO="${ELASTIC_SNAPSHOT_REPOSITORY:-found-snapshots}"
DATA_STREAM="${SWS_ONLINE_USERS_DATA_STREAM:-logs-sws.online-users-default}"
ILM_POLICY="logs-sws.online-users-ilm"
TEMPLATE_NAME="logs-sws.online-users-template"
HOT_MAX_AGE="${SWS_ONLINE_ILM_HOT_MAX_AGE:-30d}"
HOT_MAX_SHARD_SIZE="${SWS_ONLINE_ILM_HOT_MAX_SHARD_SIZE:-50gb}"

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
  "index_patterns": ["${DATA_STREAM}*", "logs-sws.online-users-*"],
  "data_stream": {},
  "priority": 200,
  "template": {
    "settings": {
      "index.lifecycle.name": "${ILM_POLICY}"
    },
    "mappings": {
      "dynamic": false,
      "properties": {
        "@timestamp":              { "type": "date" },
        "data_stream": {
          "properties": {
            "dataset":   { "type": "constant_keyword", "value": "sws.online-users" },
            "namespace": { "type": "constant_keyword", "value": "default" },
            "type":      { "type": "constant_keyword", "value": "logs" }
          }
        },
        "user_key":                { "type": "keyword" },
        "user_username":           { "type": "keyword" },
        "user_fullName":           { "type": "keyword" },
        "applications":            { "type": "keyword" },
        "session_ids":             { "type": "keyword" },
        "sessions_count":          { "type": "integer" },
        "first_session_start":     { "type": "date", "format": "epoch_millis" },
        "status":                  { "type": "keyword" }
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
    -d "{\"data_view\":{\"title\":\"${DATA_STREAM}\",\"name\":\"${KIBANA_DATA_VIEW_NAME:-SWS Online Users}\",\"timeFieldName\":\"@timestamp\"}}"
  echo
fi

echo ">> bootstrap concluido."
