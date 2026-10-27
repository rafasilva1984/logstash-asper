#!/usr/bin/env bash
# Carga FULL do prodesp-rolemembers-daily, fora do Logstash, pra deixar
# rodando no servidor. Nao precisa exportar nada nem colar chave: le sozinho o
# .env (credenciais ja preenchidas).
#
# Uso (como root):
#   /mnt/asper/scripts/run_full_load.sh            # volta na hora, segue em background
#   tail -f /var/log/logstash/prodesp-rolemembers-fullload.log
#   /mnt/asper/scripts/run_full_load.sh --status   # rodando? ultimo resumo
#   /mnt/asper/scripts/run_full_load.sh --foreground   # (interno / debug)
#
# O que faz, em rodadas (ate FULL_LOAD_ROUNDS, default 5):
#   worker (todas as roles + dump User, retry/passadas/checkpoint) -> NDJSON
#   -> push direto pro Elastic (_bulk create; repetido = 409)
#   -> le o run_summary: para quando TODAS as roles fecharam (coletado ==
#      FullCount) e o dump de User terminou; senao, nova rodada, retomando
#      do checkpoint em /var/lib/logstash/prodesp-rolemembers/.
# O snapshot_date e fixado no inicio, entao todas as rodadas caem no mesmo
# snapshot mesmo virando a meia-noite. Se o pipeline do Logstash disparar
# (03:00) enquanto isto roda, ele ve o lock e sai sem fazer nada.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SELF="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
LOG="${FULL_LOAD_LOG:-/var/log/logstash/prodesp-rolemembers-fullload.log}"

WORKER="/etc/logstash/pipelines/prodesp_rolemembers_worker.py"
[ -f "$WORKER" ] || WORKER="$SCRIPT_DIR/prodesp_rolemembers_worker.py"
PUSH_SCRIPT="$SCRIPT_DIR/prodesp_rolemembers_push.py"

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
STATE_DIR="${PRODESP_ROLEMEMBERS_STATE_DIR:-/var/lib/logstash/prodesp-rolemembers}"
PIDFILE="$STATE_DIR/fullload.pid"
WORK_DIR="$STATE_DIR/fullload"

case "${1:-}" in
  --status)
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      echo "RODANDO (pid $(cat "$PIDFILE"))"
    else
      echo "parado"
    fi
    grep -E '\[rodada|\[fim\]|resumo\]|CONCLUIDO|ERRO' "$LOG" 2>/dev/null | tail -15
    exit 0
    ;;
  --foreground) ;;
  *)
    # modo padrao: prepara como root, desacopla e devolve o terminal
    [ "$(id -u)" = "0" ] || { echo "rode como root: sudo $SELF" >&2; exit 1; }
    [ -f "$PUSH_SCRIPT" ] || { echo "ERRO: $PUSH_SCRIPT nao encontrado (precisa estar ao lado deste script)" >&2; exit 1; }
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      echo "ja esta rodando (pid $(cat "$PIDFILE")). Acompanhe: tail -f $LOG"; exit 0
    fi
    mkdir -p "$STATE_DIR" "$WORK_DIR" "$(dirname "$LOG")"
    chown -R logstash:logstash "$STATE_DIR"
    touch "$LOG"; chown logstash:logstash "$LOG"
    chown logstash:logstash "$ENV_FILE" 2>/dev/null || true
    nohup setsid runuser -u logstash -- env ENV_FILE="$ENV_FILE" FULL_LOAD_LOG="$LOG" \
      "$SELF" --foreground >>"$LOG" 2>&1 </dev/null &
    sleep 1
    echo "carga full iniciada em background (log: $LOG)"
    echo "acompanhar:  tail -f $LOG"
    echo "status:      $SELF --status"
    exit 0
    ;;
esac

# ------------------------------------------------------------ foreground --
mkdir -p "$STATE_DIR" "$WORK_DIR"
echo $$ >"$PIDFILE"
trap 'rm -f "$PIDFILE"' EXIT

ROUNDS="${FULL_LOAD_ROUNDS:-5}"
DATA_STREAM="${PRODESP_ROLEMEMBERS_DATA_STREAM:-logs-cyberark.prodesprolemembers-default}"
export PRODESP_ROLEMEMBERS_SNAPSHOT_DATE="${PRODESP_ROLEMEMBERS_SNAPSHOT_DATE:-$(TZ=America/Sao_Paulo date +%Y-%m-%d)}"
mkdir -p "$WORK_DIR"

echo "================================================================"
echo "$(date '+%F %T') carga full -- snapshot_date=${PRODESP_ROLEMEMBERS_SNAPSHOT_DATE} rodadas<=${ROUNDS}"
echo "env=${ENV_FILE} worker=${WORKER} data_stream=${DATA_STREAM}"
echo "================================================================"

for rodada in $(seq 1 "$ROUNDS"); do
  STAMP=$(date +%Y%m%d_%H%M%S)
  NDJSON="$WORK_DIR/load_${STAMP}.ndjson"
  echo
  echo "[rodada ${rodada}/${ROUNDS}] $(date '+%F %T') worker -> ${NDJSON}"
  # stderr do worker vai direto pro log (progresso role a role)
  rc=0; /usr/bin/python3 "$WORKER" >"$NDJSON" || rc=$?
  [ "$rc" = "0" ] || echo "[rodada ${rodada}] ERRO: worker saiu com rc=${rc} -- o que coletou esta no checkpoint"
  echo "[rodada ${rodada}] $(date '+%F %T') linhas no NDJSON: $(wc -l <"$NDJSON")"

  echo "[rodada ${rodada}] push -> ${DATA_STREAM}"
  if /usr/bin/python3 "$PUSH_SCRIPT" <"$NDJSON" 2>"$NDJSON.push.log"; then
    tail -1 "$NDJSON.push.log"
    rm -f "$NDJSON" "$NDJSON.push.log"
  else
    tail -20 "$NDJSON.push.log"
    echo "[rodada ${rodada}] ERRO no push -- NDJSON mantido; reenviar com:"
    echo "  python3 $PUSH_SCRIPT < $NDJSON"
    sleep 300
    continue
  fi

  # resumo da rodada (o run_summary foi emitido no proprio NDJSON; o worker
  # tambem loga [fim] no stderr). Le de novo do estado pra decidir.
  STATUS=$(STATE_DIR="$STATE_DIR" python3 - <<'PY'
import glob, json, os
d = os.path.join(os.environ["STATE_DIR"],
                 os.environ["PRODESP_ROLEMEMBERS_SNAPSHOT_DATE"])
roles = json.load(open(os.path.join(d, "roles.json"))) if os.path.exists(os.path.join(d, "roles.json")) else []
nao_ok = []
for r in roles:
    p = os.path.join(d, "roles", r["RoleId"].replace("/", "_"), "meta.json")
    m = json.load(open(p)) if os.path.exists(p) else {"status": "pendente"}
    if m.get("status") != "ok":
        nao_ok.append(f'{r["RoleName"]}={m.get("status")} {m.get("collected_count")}/{m.get("expected_count")}')
um = os.path.join(d, "users", "meta.json")
users_ok = os.path.exists(um) and json.load(open(um)).get("complete")
print(f"roles={len(roles)} nao_ok={len(nao_ok)} users_completo={bool(users_ok)}")
for x in nao_ok:
    print("  NAO FECHOU: " + x)
print("DONE" if roles and not nao_ok and users_ok else "AGAIN")
PY
)
  echo "$STATUS" | sed "s/^/[rodada ${rodada}] /"
  if echo "$STATUS" | tail -1 | grep -q DONE; then
    echo
    echo "$(date '+%F %T') CONCLUIDO: todas as roles fecharam (coletado == FullCount) e o dump de User terminou."
    exit 0
  fi
  [ "$rodada" -lt "$ROUNDS" ] && { echo "[rodada ${rodada}] nova rodada em 5 min (retoma do checkpoint)"; sleep 300; }
done

echo
echo "$(date '+%F %T') FIM SEM FECHAR TUDO apos ${ROUNDS} rodadas -- ver as linhas NAO FECHOU acima."
echo "Rodar de novo no MESMO dia continua do checkpoint: $SELF"
echo "(outro dia: PRODESP_ROLEMEMBERS_SNAPSHOT_DATE=${PRODESP_ROLEMEMBERS_SNAPSHOT_DATE} $SELF pra continuar este snapshot)"
exit 1
