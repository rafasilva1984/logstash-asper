#!/usr/bin/env bash
# Sanity-check do pipeline sws-recordings: compara a quantidade de dado
# na origem (API Alero) com a quantidade indexada no Elastic
# (logs-sws.recordings-default), na MESMA janela de tempo.
#
# Uso:
#   source envio_sws_recordings.env
#   ./validate_sws_recordings.sh [minutos]   # default: 5 (ultimos N min antes da chamada)
#
# Variaveis usadas (mesmas do worker / .env do pipeline):
#   ELASTIC_URL, ELASTIC_USER, ELASTIC_PASSWORD          (obrigatorias)
#   SWS_RECORDINGS_DATA_STREAM  (default: logs-sws.recordings-default)
#   ALERO_CLIENT_ID, ALERO_CLIENT_SECRET                 (obrigatorias)
#   ALERO_AUTH_URL   (default: https://auth.alero.io/auth/realms/serviceaccounts/protocol/openid-connect/token)
#   ALERO_BASE_URL   (default: https://api.alero.io/v2-edge)
#   ALERO_PAGE_LIMIT (default: 100)
#
# IMPORTANTE (ver CLAUDE.md 3.12): a Alero filtra /sws/recordings pelo
# horario de INICIO da sessao e so devolve sessao ja FECHADA; o Elastic
# indexa por horario de EVENTO (step), nao de inicio da sessao. Numa
# janela curta (5 min) uma pequena diferenca e esperada e normal — o
# pipeline de reconciliacao diario cobre o que escapar dessa janela.
# Divergencia GRANDE ou PERSISTENTE (repetida em varias execucoes) e que
# e sinal real de problema na ingestao.

set -euo pipefail

MINUTES="${1:-5}"

: "${ELASTIC_URL:?defina ELASTIC_URL}"
: "${ELASTIC_USER:?defina ELASTIC_USER}"
: "${ELASTIC_PASSWORD:?defina ELASTIC_PASSWORD}"
: "${ALERO_CLIENT_ID:?defina ALERO_CLIENT_ID}"
: "${ALERO_CLIENT_SECRET:?defina ALERO_CLIENT_SECRET}"

DATA_STREAM="${SWS_RECORDINGS_DATA_STREAM:-logs-sws.recordings-default}"
ALERO_AUTH_URL="${ALERO_AUTH_URL:-https://auth.alero.io/auth/realms/serviceaccounts/protocol/openid-connect/token}"
ALERO_BASE_URL="${ALERO_BASE_URL:-https://api.alero.io/v2-edge}"
ALERO_PAGE_LIMIT="${ALERO_PAGE_LIMIT:-100}"

NOW_MS=$(($(date -u +%s%N) / 1000000))
FROM_MS=$((NOW_MS - MINUTES * 60000))

iso() { python3 -c "import sys,datetime; print(datetime.datetime.fromtimestamp(int(sys.argv[1])/1000, tz=datetime.timezone.utc).isoformat())" "$1"; }
FROM_ISO=$(iso "$FROM_MS")
NOW_ISO=$(iso "$NOW_MS")

echo "== janela: ${FROM_ISO} -> ${NOW_ISO} (ultimos ${MINUTES} min) =="
echo

# --- Alero: token ---
echo ">> autenticando na Alero..."
ALERO_TOKEN=$(curl -sS -X POST "$ALERO_AUTH_URL" \
  --data-urlencode "client_id=${ALERO_CLIENT_ID}" \
  --data-urlencode "client_secret=${ALERO_CLIENT_SECRET}" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "scope=openid" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin).get("access_token",""))')

if [ -z "$ALERO_TOKEN" ]; then
  echo "ERRO: nao consegui obter token da Alero" >&2
  exit 1
fi

# --- Alero: pagina /sws/recordings na janela, soma sessoes e docs esperados ---
echo ">> consultando Alero (/sws/recordings, fromTime=${FROM_MS}, toTime=${NOW_MS})..."
ALERO_RESULT=$(python3 - "$ALERO_BASE_URL" "$ALERO_TOKEN" "$FROM_MS" "$NOW_MS" "$ALERO_PAGE_LIMIT" <<'PYEOF'
import sys, json, urllib.request

base_url, token, from_ms, to_ms, limit = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
offset = 0
total_recordings = 0
total_docs_esperados = 0  # 1 doc por step; recording sem step vira 1 doc sintetico (ver worker)

while True:
    url = f"{base_url}/sws/recordings?fromTime={from_ms}&toTime={to_ms}&offset={offset}&limit={limit}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.load(r)
    batch = data.get("recordings", [])
    if not batch:
        break
    total_recordings += len(batch)
    for rec in batch:
        total_docs_esperados += max(rec.get("stepsCount") or 0, 1)
    if len(batch) < limit:
        break
    offset += limit

print(json.dumps({"recordings": total_recordings, "docs_esperados": total_docs_esperados}))
PYEOF
)

ALERO_RECORDINGS=$(echo "$ALERO_RESULT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["recordings"])')
ALERO_DOCS_ESPERADOS=$(echo "$ALERO_RESULT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["docs_esperados"])')

# --- Elastic: total de documentos com @timestamp na janela ---
echo ">> consultando Elastic (${DATA_STREAM}, @timestamp na janela)..."
ES_COUNT=$(curl -sS -u "${ELASTIC_USER}:${ELASTIC_PASSWORD}" -H 'Content-Type: application/json' \
  "${ELASTIC_URL}/${DATA_STREAM}/_count" \
  -d "{\"query\":{\"range\":{\"@timestamp\":{\"gte\":\"${FROM_ISO}\",\"lte\":\"${NOW_ISO}\"}}}}" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin).get("count","?"))')

# --- Elastic: recordings distintas (cardinality) na mesma janela ---
ES_RECORDINGS=$(curl -sS -u "${ELASTIC_USER}:${ELASTIC_PASSWORD}" -H 'Content-Type: application/json' \
  "${ELASTIC_URL}/${DATA_STREAM}/_search" \
  -d "{\"size\":0,\"query\":{\"range\":{\"@timestamp\":{\"gte\":\"${FROM_ISO}\",\"lte\":\"${NOW_ISO}\"}}},\"aggs\":{\"recs\":{\"cardinality\":{\"field\":\"recording_id\"}}}}" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["aggregations"]["recs"]["value"])')

echo
echo "== resultado =="
printf '%-40s %12s %12s\n' "" "Alero" "Elastic"
printf '%-40s %12s %12s\n' "recordings (sessoes) na janela" "$ALERO_RECORDINGS" "$ES_RECORDINGS"
printf '%-40s %12s %12s\n' "documentos (steps, min 1/recording)" "$ALERO_DOCS_ESPERADOS" "$ES_COUNT"
echo
echo "Nota: Alero filtra por INICIO da sessao (so fechada); Elastic indexa por"
echo "horario de EVENTO (step). Diferenca pequena numa janela de ${MINUTES} min e"
echo "esperada (CLAUDE.md 3.12) — o reconcile diario cobre o que escapar aqui."
