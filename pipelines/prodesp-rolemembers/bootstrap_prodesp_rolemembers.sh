#!/usr/bin/env bash
# Bootstrap do ILM policy + index template + data stream do pipeline
# prodesp-rolemembers-daily (CyberArk Prodesp, membros por role via relatorio
# @/lib/reports/compliance/mfa.js). Idempotente: reexecutar atualiza policy e
# template; o data stream so e criado se nao existir.
#
# Pipeline NOVO e isolado: nao toca em nada do cyberark-prodesp-directory
# (logs-cyberark.prodespdirectory-default), que traz o mesmo dado por outra
# regra (GetRoleMembers). Os dois rodam lado a lado para comparar.
#
# Retencao: hot PRODESP_ROLEMEMBERS_ILM_HOT_DAYS (default 7) -> frozen
# (searchable_snapshot), SEM delete (snapshot diario = historico de auditoria,
# mesmo padrao do usersselo / prodesp-directory).
#
# Uso (le o .env sozinho, nada de export):
#   sudo ./bootstrap_prodesp_rolemembers.sh
#
# Variaveis:
#   ELASTIC_URL, ELASTIC_USER, ELASTIC_PASSWORD          (obrigatorias)
#   ELASTIC_SNAPSHOT_REPOSITORY               (default: found-snapshots)
#   PRODESP_ROLEMEMBERS_DATA_STREAM           (default: logs-cyberark.prodesprolemembers-default)
#   PRODESP_ROLEMEMBERS_ILM_ROLLOVER_MAX_AGE  (default: 7d)
#   PRODESP_ROLEMEMBERS_ILM_HOT_DAYS          (default: 7)
#   KIBANA_URL, KIBANA_DATA_VIEW_NAME         (opcionais, criam o data view)

set -euo pipefail

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

: "${ELASTIC_URL:?defina ELASTIC_URL}"
: "${ELASTIC_USER:?defina ELASTIC_USER}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"

SNAPSHOT_REPO="${ELASTIC_SNAPSHOT_REPOSITORY:-found-snapshots}"
DATA_STREAM="${PRODESP_ROLEMEMBERS_DATA_STREAM:-logs-cyberark.prodesprolemembers-default}"
ILM_POLICY="logs-cyberark.prodesprolemembers-ilm"
TEMPLATE_NAME="logs-cyberark.prodesprolemembers-template"
ROLLOVER_MAX_AGE="${PRODESP_ROLEMEMBERS_ILM_ROLLOVER_MAX_AGE:-7d}"
HOT_DAYS="${PRODESP_ROLEMEMBERS_ILM_HOT_DAYS:-7}"

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

# Campos (doc_type member / user / role_summary / run_summary). Reaplicado
# tambem no data stream existente (PUT _mapping), pra campo novo valer sem
# esperar o rollover.
read -r -d '' MAPPING_PROPERTIES <<'JSON' || true
{
  "@timestamp":          { "type": "date" },
  "data_stream": {
    "properties": {
      "dataset":   { "type": "constant_keyword", "value": "cyberark.prodesprolemembers" },
      "namespace": { "type": "constant_keyword", "value": "default" },
      "type":      { "type": "constant_keyword", "value": "logs" }
    }
  },
  "doc_type":            { "type": "keyword" },
  "snapshot_date":       { "type": "date", "format": "yyyy-MM-dd" },
  "run_id":              { "type": "keyword" },
  "RoleId":              { "type": "keyword" },
  "RoleName":            { "type": "keyword", "fields": { "text": { "type": "text" } } },
  "Username":            { "type": "keyword" },
  "IdentityProfile":     { "type": "keyword" },
  "IdentityChallenge1":  { "type": "keyword" },
  "IdentityChallenge2":  { "type": "keyword" },
  "RadiusProfile":       { "type": "keyword" },
  "RadiusChallenge1":    { "type": "keyword" },
  "RadiusChallenge2":    { "type": "keyword" },
  "UserId":              { "type": "keyword" },
  "UserStatus":          { "type": "keyword" },
  "LastLogin":           { "type": "date" },
  "LastLogin_raw":       { "type": "keyword" },
  "role_count":          { "type": "integer" },
  "has_role":            { "type": "boolean" },
  "roles_complete":      { "type": "boolean" },
  "status":              { "type": "keyword" },
  "expected_count":      { "type": "long" },
  "collected_count":     { "type": "long" },
  "attempts":            { "type": "integer" },
  "restarts":            { "type": "integer" },
  "error":               { "type": "keyword", "ignore_above": 2048 },
  "roles_total":         { "type": "integer" },
  "roles_ok":            { "type": "integer" },
  "roles_not_ok":        { "type": "integer" },
  "members_emitted":     { "type": "long" },
  "users_emitted":       { "type": "long" },
  "users_without_role":  { "type": "long" },
  "users_complete":      { "type": "boolean" },
  "source_method":       { "type": "keyword" },
  "rolemember_count":    { "type": "long" },
  "mfa_full_count":      { "type": "long" },
  "rm_full_count":       { "type": "long" },
  "mfa_error":           { "type": "keyword", "ignore_above": 2048 },
  "roles_mfa":           { "type": "integer" },
  "roles_rm":            { "type": "integer" },
  "roles_rm_deferred":   { "type": "integer" },
  "duration_s":          { "type": "float" }
}
JSON

echo ">> criando/atualizando index template '${TEMPLATE_NAME}'..."
put_json "${ELASTIC_URL}/_index_template/${TEMPLATE_NAME}" <<EOF
{
  "index_patterns": ["logs-cyberark.prodesprolemembers-*"],
  "data_stream": {},
  "priority": 200,
  "template": {
    "settings": {
      "index.lifecycle.name": "${ILM_POLICY}"
    },
    "mappings": {
      "dynamic": false,
      "properties": ${MAPPING_PROPERTIES}
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

echo ">> aplicando o mapping nos indices backing ja existentes..."
put_json "${ELASTIC_URL}/${DATA_STREAM}/_mapping" <<EOF
{ "properties": ${MAPPING_PROPERTIES} }
EOF

echo ">> conferindo: indice backing usa a policy?"
curl -sS "${AUTH[@]}" "${ELASTIC_URL}/_data_stream/${DATA_STREAM}" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin)["data_streams"][0]; print("   template=",d["template"],"| ilm_policy=",d.get("ilm_policy"),"| backing=",[i["index_name"] for i in d["indices"]])'

if [ -n "${KIBANA_URL:-}" ]; then
  echo ">> criando data view no Kibana..."
  curl -sS "${AUTH[@]}" -X POST "${KIBANA_URL}/api/data_views/data_view" \
    -H 'Content-Type: application/json' -H 'kbn-xsrf: true' \
    -d "{\"data_view\":{\"title\":\"${DATA_STREAM}\",\"name\":\"${KIBANA_DATA_VIEW_NAME:-CyberArk Prodesp - Membros por Role}\",\"timeFieldName\":\"@timestamp\"}}"
  echo
fi

echo ">> bootstrap concluido."
