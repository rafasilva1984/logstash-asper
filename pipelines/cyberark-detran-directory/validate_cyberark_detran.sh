#!/usr/bin/env bash
# Sanity-check do pipeline cyberark-detran-directory: compara os totais
# na origem (CyberArk tenant Detran) com o que foi indexado no Elastic
# para o snapshot_date informado (default: hoje, BRT).
#
# Uso:
#   source envio_cyberark_detran.env
#   ./validate_cyberark_detran.sh              # snapshot de hoje (BRT)
#   ./validate_cyberark_detran.sh 2026-08-23    # snapshot de um dia especifico
#
# Variaveis usadas (mesmas do worker / .env do pipeline):
#   ELASTIC_URL, ELASTIC_USER, ELASTIC_PASSWORD          (obrigatorias)
#   DETRAN_CLIENT_ID, DETRAN_CLIENT_SECRET             (obrigatorias)
#   DETRAN_TOKEN_URL, DETRAN_QUERY_URL, DETRAN_ROLE_MEMBERS_URL (defaults do worker)
#   DETRAN_DATA_STREAM                                  (default do worker)
#
# NOTA (ver worker/DEPLOY.md): a origem de roles NAO usa mais
# COUNT(RoleMember.ID) — essa tabela nao e consultavel em bulk nesse
# tenant (trava). A contagem de origem soma o FullCount de
# /Roles/GetRoleMembers por role (mesmo endpoint que o worker usa).

set -euo pipefail

: "${ELASTIC_URL:?defina ELASTIC_URL}"
: "${ELASTIC_USER:?defina ELASTIC_USER}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"
: "${DETRAN_CLIENT_ID:?defina DETRAN_CLIENT_ID}"
: "${DETRAN_CLIENT_SECRET:?defina DETRAN_CLIENT_SECRET}"

TOKEN_URL="${DETRAN_TOKEN_URL:-https://abb4724.id.cyberark.cloud/OAuth2/Token/PainelDetran}"
QUERY_URL="${DETRAN_QUERY_URL:-https://abb4724.id.cyberark.cloud/Redrock/query}"
ROLE_MEMBERS_URL="${DETRAN_ROLE_MEMBERS_URL:-https://abb4724.id.cyberark.cloud/Roles/GetRoleMembers}"
DATA_STREAM="${DETRAN_DATA_STREAM:-logs-cyberark.detrandirectory-default}"

# snapshot_date default = hoje em BRT (UTC-3, sem horario de verao)
SNAPSHOT_DATE="${1:-$(date -u -d '3 hours ago' +%Y-%m-%d)}"

echo "== snapshot_date: ${SNAPSHOT_DATE} =="
echo

echo ">> autenticando no tenant Detran..."
TOKEN=$(curl -sS -X POST "$TOKEN_URL" \
  --data-urlencode "client_id=${DETRAN_CLIENT_ID}" \
  --data-urlencode "client_secret=${DETRAN_CLIENT_SECRET}" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "scope=all" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin).get("access_token",""))')

if [ -z "$TOKEN" ]; then
  echo "ERRO: nao consegui obter token do tenant Detran" >&2
  exit 1
fi

query_count() {
  local script="$1"
  curl -sS --max-time 120 -X POST "$QUERY_URL" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"Script\":\"${script}\",\"Args\":{\"PageNumber\":1,\"PageSize\":1,\"Caching\":-1}}" \
    | python3 -c '
import sys, json
d = json.load(sys.stdin)
rows = d.get("Result", {}).get("Results", [])
print(rows[0]["Row"].get("Total", "?") if rows else "?")
'
}

es_search() {
  # $1 = corpo da query (Query DSL); imprime o _search cru
  curl -sS -u "${ELASTIC_USER}:${ELASTIC_PASSWORD}" -H 'Content-Type: application/json' \
    "${ELASTIC_URL}/${DATA_STREAM}/_search" -d "$1"
}

echo ">> contando origem (User)..."
ORIGIN_USERS=$(query_count 'SELECT COUNT(User.Username) AS Total FROM User')

echo ">> contando origem (roles: soma de GetRoleMembers em cada uma das ~57 roles, mesmo endpoint do worker)..."
ROLE_TOTALS=$(python3 - "$QUERY_URL" "$ROLE_MEMBERS_URL" "$TOKEN" <<'PYEOF'
import sys, json, urllib.request

query_url, role_members_url, token = sys.argv[1], sys.argv[2], sys.argv[3]
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def post(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=45) as r:
        return json.load(r)

roles_body = {"Script": "SELECT Role.ID, Role.Name FROM Role", "Args": {"PageNumber": 1, "PageSize": 2000, "Caching": -1}}
roles = [item["Row"] for item in post(query_url, roles_body).get("Result", {}).get("Results", [])]

total_pares = 0
usuarios_distintos = set()
for role in roles:
    role_id = role.get("ID")
    body = {"Name": role_id, "Args": {"PageNumber": 1, "PageSize": 1, "Caching": -1}}
    try:
        result = post(role_members_url, body).get("Result", {})
    except Exception as e:
        print(f"aviso: role {role_id} falhou ({e})", file=sys.stderr)
        continue
    full_count = result.get("FullCount", 0) or 0
    total_pares += full_count
    # so a 1a pagina (PageSize:1) nao da pra listar os distintos aqui sem
    # paginar tudo; a contagem de "usuarios distintos com role" fica por
    # conta da comparacao com o Elastic (cardinality), nao recalculada aqui.

print(json.dumps({"total_pares": total_pares, "total_roles": len(roles)}))
PYEOF
)

ORIGIN_ROLE_ROWS=$(echo "$ROLE_TOTALS" | python3 -c 'import sys,json; print(json.load(sys.stdin)["total_pares"])')
ORIGIN_TOTAL_ROLES=$(echo "$ROLE_TOTALS" | python3 -c 'import sys,json; print(json.load(sys.stdin)["total_roles"])')

echo ">> consultando Elastic (${DATA_STREAM}, snapshot_date=${SNAPSHOT_DATE})..."
ES_RESULT=$(es_search "{
  \"size\": 0,
  \"track_total_hits\": true,
  \"query\": { \"term\": { \"snapshot_date\": \"${SNAPSHOT_DATE}\" } },
  \"aggs\": {
    \"usuarios_distintos\": { \"cardinality\": { \"field\": \"user_id\" } },
    \"com_role\": { \"filter\": { \"exists\": { \"field\": \"role_id\" } } },
    \"sem_role\": { \"filter\": { \"bool\": { \"must_not\": { \"exists\": { \"field\": \"role_id\" } } } } }
  }
}")

ES_TOTAL_DOCS=$(echo "$ES_RESULT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["hits"]["total"]["value"])')
ES_USUARIOS=$(echo "$ES_RESULT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["aggregations"]["usuarios_distintos"]["value"])')
ES_COM_ROLE=$(echo "$ES_RESULT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["aggregations"]["com_role"]["doc_count"])')
ES_SEM_ROLE=$(echo "$ES_RESULT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["aggregations"]["sem_role"]["doc_count"])')

echo
echo "== resultado =="
printf '%-45s %15s %15s\n' "" "Origem (CyberArk)" "Elastic (snapshot)"
printf '%-45s %15s %15s\n' "usuarios (total)"                         "$ORIGIN_USERS"      "$ES_USUARIOS"
printf '%-45s %15s %15s\n' "roles processadas"                        "$ORIGIN_TOTAL_ROLES" "-"
printf '%-45s %15s %15s\n' "pares usuario+role (docs com role_id)"     "$ORIGIN_ROLE_ROWS"  "$ES_COM_ROLE"
printf '%-45s %15s %15s\n' "docs sem role (role_id null)"              "-"                  "$ES_SEM_ROLE"
printf '%-45s %15s %15s\n' "total de documentos no snapshot"           "-"                  "$ES_TOTAL_DOCS"
echo
echo "Esperado: ES_COM_ROLE ~= ORIGIN_ROLE_ROWS (pode ser levemente MAIOR na origem,"
echo "porque o worker so conta Type=='User' e ignora membros do tipo Role aninhada —"
echo "ver log '[roles][<id>] N membro(s) do tipo Role' no worker); ES_USUARIOS ~="
echo "ORIGIN_USERS. Pequena diferenca e esperada (a origem e consultada 'agora', o"
echo "snapshot rodou as 20h BRT). Diferenca grande e sinal de problema real."
