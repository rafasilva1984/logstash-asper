#!/usr/bin/env bash
# Bootstrap do ILM policy + index template + data stream do pipeline
# prodesp-applaunch-realtime (CyberArk Prodesp, AppLaunch + dados do User).
# Idempotente: reexecutar atualiza policy e template; o data stream so e
# criado se nao existir.
#
# Retencao: 30 dias hot -> frozen (searchable_snapshot), SEM delete
# (auditoria/compliance, padrao da plataforma).
#
# min_age da fase frozen conta a partir do ROLLOVER do indice backing. Com
# rollover a cada PRODESP_APPLAUNCH_ILM_ROLLOVER_MAX_AGE (default 7d), cada
# dado fica hot entre 30 e 37 dias.
#
# Uso:
#   set -a; source /etc/logstash/envio_prodesp_applaunch.env; set +a
#   ./bootstrap_prodesp_applaunch.sh
#
# Variaveis:
#   ELASTIC_URL, ELASTIC_USER, ELASTIC_PASSWORD        (obrigatorias)
#   ELASTIC_SNAPSHOT_REPOSITORY             (default: found-snapshots)
#   PRODESP_APPLAUNCH_DATA_STREAM           (default: logs-cyberark.prodespapplaunch-default)
#   PRODESP_APPLAUNCH_ILM_ROLLOVER_MAX_AGE  (default: 7d)
#   PRODESP_APPLAUNCH_ILM_HOT_DAYS          (default: 30)
#   KIBANA_URL, KIBANA_DATA_VIEW_NAME       (opcionais, criam o data view)

set -euo pipefail

: "${ELASTIC_URL:?defina ELASTIC_URL}"
: "${ELASTIC_USER:?defina ELASTIC_USER}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"

SNAPSHOT_REPO="${ELASTIC_SNAPSHOT_REPOSITORY:-found-snapshots}"
DATA_STREAM="${PRODESP_APPLAUNCH_DATA_STREAM:-logs-cyberark.prodespapplaunch-default}"
ILM_POLICY="logs-cyberark.prodespapplaunch-ilm"
TEMPLATE_NAME="logs-cyberark.prodespapplaunch-template"
ROLLOVER_MAX_AGE="${PRODESP_APPLAUNCH_ILM_ROLLOVER_MAX_AGE:-7d}"
HOT_DAYS="${PRODESP_APPLAUNCH_ILM_HOT_DAYS:-30}"

AUTH=(-u "${ELASTIC_USER}:${ELASTIC_PASSWORD}")
OUT=$(mktemp)
trap 'rm -f "$OUT"' EXIT

# PUT que falha alto: curl -sS sozinho nao da erro em HTTP 4xx/5xx.
put_json() {
  local url="$1" body code
  body=$(cat)
  code=$(curl -sS "${AUTH[@]}" -o "$OUT" -w '%{http_code}' \
    -X PUT "$url" -H 'Content-Type: application/json' -d "$body")
  cat "$OUT"; echo
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

echo ">> criando/atualizando ILM policy '${ILM_POLICY}' (hot ${HOT_DAYS}d -> frozen, sem delete)..."
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
      }
    }
  }
}
EOF

echo ">> criando/atualizando index template '${TEMPLATE_NAME}'..."
put_json "${ELASTIC_URL}/_index_template/${TEMPLATE_NAME}" <<EOF
{
  "index_patterns": ["logs-cyberark.prodespapplaunch-*"],
  "data_stream": {},
  "priority": 200,
  "template": {
    "settings": {
      "index.lifecycle.name": "${ILM_POLICY}"
    },
    "mappings": {
      "dynamic": false,
      "properties": {
        "@timestamp":         { "type": "date" },
        "data_stream": {
          "properties": {
            "dataset":   { "type": "constant_keyword", "value": "cyberark.prodespapplaunch" },
            "namespace": { "type": "constant_keyword", "value": "default" },
            "type":      { "type": "constant_keyword", "value": "logs" }
          }
        },
        "LoginName":          { "type": "keyword" },
        "DisplayName":        { "type": "keyword", "fields": { "text": { "type": "text" } } },
        "Status":             { "type": "keyword" },
        "UserType":           { "type": "keyword" },
        "UserID":             { "type": "keyword" },
        "Selo_":              { "type": "keyword", "ignore_above": 4096 },
        "Selo_level":         { "type": "keyword" },
        "Selo_reliabilities": {
          "properties": {
            "id":        { "type": "keyword" },
            "updatedAt": { "type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSSZ||strict_date_optional_time", "ignore_malformed": true }
          }
        },
        "LastLogin":          { "type": "date" },
        "user_found":         { "type": "boolean" },
        "ClientIPAddress":    { "type": "ip", "ignore_malformed": true },
        "FromIPAddress":      { "type": "ip", "ignore_malformed": true },
        "Country":            { "type": "keyword" },
        "City":               { "type": "keyword" },
        "Latitude":           { "type": "float" },
        "Longitude":          { "type": "float" },
        "Location":           { "type": "geo_point", "ignore_malformed": true },
        "ApplicationName":    { "type": "keyword" },
        "ApplicationType":    { "type": "keyword" },
        "AppName":            { "type": "keyword" },
        "EventType":          { "type": "keyword" },
        "WhenLogged":         { "type": "date" },
        "WhenOccurred":       { "type": "date" },
        "Uuid":               { "type": "keyword" }
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
  code=$(curl -sS "${AUTH[@]}" -o "$OUT" -w '%{http_code}' -X PUT "${ELASTIC_URL}/_data_stream/${DATA_STREAM}")
  cat "$OUT"; echo
  [ "$code" = "200" ] || { echo "ERRO: HTTP $code criando data stream" >&2; exit 1; }
fi

echo ">> conferindo: indice backing usa a policy?"
curl -sS "${AUTH[@]}" "${ELASTIC_URL}/_data_stream/${DATA_STREAM}" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin)["data_streams"][0]; print("   template=",d["template"],"| ilm_policy=",d.get("ilm_policy"),"| backing=",[i["index_name"] for i in d["indices"]])'

if [ -n "${KIBANA_URL:-}" ]; then
  echo ">> criando data view no Kibana..."
  curl -sS "${AUTH[@]}" -X POST "${KIBANA_URL}/api/data_views/data_view" \
    -H 'Content-Type: application/json' -H 'kbn-xsrf: true' \
    -d "{\"data_view\":{\"title\":\"${DATA_STREAM}\",\"name\":\"${KIBANA_DATA_VIEW_NAME:-CyberArk Prodesp - AppLaunch}\",\"timeFieldName\":\"@timestamp\"}}"
  echo
fi

echo ">> bootstrap concluido."
