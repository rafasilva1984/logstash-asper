# Deploy — pipeline `prodesp-rolemembers-daily` (novo, CyberArk tenant Prodesp)

Runbook de produção. Leia antes o `CLAUDE.md` da raiz (§3 e §10).

## O que é

Porta o script do cliente que gerava `baserole_membros.csv` (`Username;RoleName`):
catálogo de roles (`SELECT Role.Name, Role.ID FROM Role`) e, **para cada role**,
o relatório pronto `@/lib/reports/compliance/mfa.js` com o parâmetro `roleId`.
O relatório devolve os membros da role (Username + perfil/challenges de MFA).
Além disso, faz o **dump da tabela `User`**, para incluir quem não tem role.

É o **mesmo dado** do `cyberark-prodesp-directory` (quem tem qual role), mas por
**outra regra**. Aquele usa `POST /Roles/GetRoleMembers`: só membros diretos do
tipo User, roles aninhadas ignoradas e ~8% das roles dando timeout e sumindo do
snapshot (ver o DEPLOY.md dele). Percebemos perda de dados lá. Este pipeline é
**novo e isolado** e roda lado a lado para comparar. O antigo não é alterado.

| Item | Valor |
|------|-------|
| pipeline.id | `prodesp-rolemembers-daily` (workers=1, tem estado) |
| Worker / conf | `prodesp_rolemembers_worker.py` / `prodesp-rolemembers-daily.conf` |
| Codec | `json_lines` + heartbeat (dump grande, CLAUDE.md 3.1/3.2) |
| Agendamento | `schedule` `0 3 * * *`: **03:00** hora local do servidor, 1x/dia |
| Data stream | `logs-cyberark.prodesprolemembers-default` |
| ILM | `logs-cyberark.prodesprolemembers-ilm`: hot 7d (rollover 7d) → frozen, **sem delete** |
| Template | `logs-cyberark.prodesprolemembers-template` |
| `.env` | `/etc/logstash/envio_prodesp_rolemembers.env` (prefixo `PRODESP_ROLEMEMBERS_`) |
| Estado/checkpoint | `/var/lib/logstash/prodesp-rolemembers/` (dono logstash) |
| 1ª carga full | `run_full_load.sh`, fora do Logstash, roda em background no servidor |

**Credenciais**: o `envio_prodesp_rolemembers.env` real desta pasta já vem
preenchido (Elastic + client `elastic_bi_asper`). Ele é gitignored e está só
na estação: copie com `scp`. Todos os scripts leem o `.env` sozinhos, **sem
`export` e sem colar chave**.

**ILM, template e data stream já foram criados no Elastic em 2026-10-26**
(`bootstrap_prodesp_rolemembers.sh` rodado da estação). O data stream já tem
669 docs `member` reais de teste (snapshot 2026-10-26), com o mesmo `_id` que o
pipeline vai gerar.

## Regra de completude ("não deixar nada de fora")

1. **Nenhuma role é pulada**, inclusive `Everybody`. A `Everybody` só não conta
   no `role_count`/`has_role` do doc `user` (`IMPLICIT_ROLES`), porque todo
   mundo é membro dela.
2. **Página grande e timeout alto**: `PAGE_SIZE=5000`, `TIMEOUT_SECONDS=3600`
   (1h por chamada, **em teste**).
3. **A role só fecha quando coletado == FullCount** (usuários distintos).
   Senão ela é refeita: retry por chamada com backoff exponencial
   (`MAX_RETRIES=5`, 15s → 300s) e várias passadas sobre as roles abertas
   (`MAX_PASSES=10`, com espera crescente entre elas). Se a paginação terminar
   e não bater (a lista mudou no meio), a role é refeita do zero. O que já
   tinha sido visto vai para `extras.json` e também é emitido, porque são
   membros reais vistos no dia.
4. **Checkpoint por role/página** no estado do dia
   (`<STATE_DIR>/<snapshot_date>/roles/<RoleId>/pNNNNN.json` + `meta.json`).
   O checkpoint guarda **as linhas**, não só "página feita". O `exec` só entrega
   o stdout ao Logstash quando o processo termina (CLAUDE.md 3.11). Se a
   execução cair, o que ela coletou está no disco. A próxima execução do mesmo
   snapshot retoma das páginas que faltam e **reemite tudo** que está no estado.
   O `_id` determinístico transforma a repetição em 409. Testado em 2026-10-26:
   worker morto com `kill -9` no meio de uma role (stdout perdido, 0 linhas); a
   execução seguinte continuou da página 5 e emitiu 520/520, sem `_id` repetido.
5. **Paralelismo menor nas roles pesadas**: duas filas simultâneas, leves
   (`LIGHT_WORKERS=4`) e pesadas (`HEAVY_WORKERS=1`). Pesada = FullCount ≥
   2000, chamada ≥ 60s ou já falhou, segundo o histórico em
   `<STATE_DIR>/role_stats.json`. Antes de existir histórico, vale o seed
   `prodesp_rolemembers_role_stats.seed.json`, medido no probe: **59 roles
   pesadas** já entram na fila lenta desde a 1ª carga.
6. **Dump da tabela User** (checkpoint por página também). Gera um doc `user`
   por usuário, com `role_count` e `has_role`. Só é emitido quando todas as
   roles que contam fecharam. Antes disso o `role_count` seria parcial, e como
   o `_id` é por dia, o valor errado ficaria gravado.
7. **Log final e `role_summary`** acusam qualquer role com coletado < FullCount
   (`[fim] NAO FECHOU: ...`). Há também um `run_summary` por execução.

Limite de tempo: cada execução para de buscar em `MAX_RUNTIME_MINUTES=1380`
(23h, termina antes do próximo disparo das 03:00) e emite o que tem. Duas
execuções nunca rodam juntas: há um lock (`flock`) no `STATE_DIR`, e a segunda
sai na hora sem fazer nada. Estado de dias anteriores é apagado após
`KEEP_DAYS=3`.

## Documentos gerados

| `doc_type` | Um por | `_id` | Campos principais |
|------------|--------|-------|-------------------|
| `member` | par (role, usuário) no dia | `member\|snapshot_date\|RoleId\|Username` | `RoleId`, `RoleName`, `Username`, `IdentityProfile`, `IdentityChallenge1/2`, `RadiusProfile`, `RadiusChallenge1/2` |
| `user` | usuário da tabela User no dia | `user\|snapshot_date\|UserId` | `Username`, `UserId`, `UserStatus`, `LastLogin`, `role_count`, `has_role`, `roles_complete` |
| `role_summary` | role por **execução** | `summary\|run_id\|RoleId` | `status` (ok/incompleto/erro/pendente), `expected_count`, `collected_count`, `attempts`, `restarts`, `error` |
| `run_summary` | execução | `run\|run_id` | `roles_ok`, `roles_not_ok`, `expected_count`, `collected_count`, `members_emitted`, `users_emitted`, `users_without_role`, `users_complete`, `duration_s` |

Todos levam `snapshot_date` (data BRT do disparo). `@timestamp` = momento do
disparo (`snapshot_ms`). Usuários sem role: `doc_type:user and has_role:false`.

## Probe (2026-10-26, da estação)

- `Role`: **224 roles**, 0,4s.
- `mfa.js` por role, PageSize 100, timeout 60s, 6 em paralelo: **166 ok**
  (somando 11.407 membros) e **58 com timeout**, entre elas todas as grandes
  (SEDUC, Poupatempo, PGE, Workspace, SOU.SP, Everybody...).
- Paginação confere: `SES - Remedio` (FullCount 505) em 6 páginas de 100 = 505
  distintos. Com PageSize 1000, veio tudo numa chamada (16s), o mesmo conjunto.
- Custo cresce com o tamanho da role: `PGE - Attus` (2.079) em 80s numa
  página; `Workspace - SGP` (5.779) em 217s para as primeiras 5.000.
  `SEDUC - Tutoria` com PageSize 5000 **passou de 905s** sem resposta. Por isso
  o timeout foi para 1h. **Não sabemos ainda** quanto SEDUC/Everybody levam de
  fato: a 1ª carga full vai mostrar (linhas `pagina N: ... em Xs` no log). Se
  alguma passar de 1h mesmo assim, aumente `TIMEOUT_SECONDS` no `.env`.
- Colunas do relatório: `Username`, `Identity Profile`, `Identity Challenge1/2`,
  `Radius Profile`, `Radius Challenge1/2`, mais o `FullCount`.
- Dump de `User`: 1.000–2.000 linhas em ~45s (o prodesp-directory mediu ~15s
  fixos + ~0,8ms/linha; ~1,25M usuários ≈ 20–25 min).

## Passo a passo (no `logstash-prod-01`)

### 1. Copiar arquivos

```bash
cd pipelines/prodesp-rolemembers
scp prodesp_rolemembers_worker.py prodesp-rolemembers-daily.conf prodesp_rolemembers_role_stats.seed.json \
    root@logstash-prod-01:/etc/logstash/pipelines/
scp bootstrap_prodesp_rolemembers.sh run_full_load.sh run_load_now.sh prodesp_rolemembers_push.py \
    root@logstash-prod-01:/mnt/asper/scripts/
scp envio_prodesp_rolemembers.env \
    root@logstash-prod-01:/etc/logstash/envio_prodesp_rolemembers.env   # o REAL, ja com credenciais
```

No servidor:

```bash
python3 -c "import ast; ast.parse(open('/etc/logstash/pipelines/prodesp_rolemembers_worker.py').read()); print('OK')"
chmod +x /mnt/asper/scripts/bootstrap_prodesp_rolemembers.sh /mnt/asper/scripts/run_full_load.sh /mnt/asper/scripts/run_load_now.sh
chown logstash:logstash /etc/logstash/envio_prodesp_rolemembers.env
chmod 600 /etc/logstash/envio_prodesp_rolemembers.env
grep SCHEDULE /etc/logstash/envio_prodesp_rolemembers.env   # tem que ser "0 3 * * *"
mkdir -p /var/lib/logstash/prodesp-rolemembers
chown logstash:logstash /var/lib/logstash/prodesp-rolemembers
```

O nome `run_load_now.sh` também existe no applaunch e no sgp/detran-steps. Se
precisar, copie com sufixo: `run_load_now_rolemembers.sh`. O
`run_full_load.sh` e o `run_load_now.sh` procuram o `prodesp_rolemembers_push.py`
na mesma pasta.

Espaço em disco: as páginas de `User` (~1,25M linhas) ocupam algumas centenas
de MB por dia. Com `KEEP_DAYS=3`, conte com ~1–2 GB em
`/var/lib/logstash/prodesp-rolemembers/`.

### 2. Bootstrap

Já foi rodado. Reexecute só se mudar a policy ou o template. É idempotente e
lê o `.env` sozinho:

```bash
/mnt/asper/scripts/bootstrap_prodesp_rolemembers.sh
```

### 3. Primeira carga FULL (deixar rodando)

```bash
/mnt/asper/scripts/run_full_load.sh
```

O script volta na hora e segue em background, como usuário `logstash`, mesmo
que você feche a sessão SSH. Ele roda o worker (todas as roles, retry,
passadas, checkpoint, dump de User), manda o NDJSON direto pro Elastic e
repete, retomando do checkpoint, até **todas as roles fecharem** e o dump de
User terminar (no máximo `FULL_LOAD_ROUNDS=5` rodadas). O `snapshot_date` é
fixado no início.

```bash
tail -f /var/log/logstash/prodesp-rolemembers-fullload.log   # progresso role a role
/mnt/asper/scripts/run_full_load.sh --status                  # rodando? ultimo resumo
```

No fim, o log mostra `CONCLUIDO` ou a lista `NAO FECHOU: <role>=<status>
coletado/FullCount`. Se parar sem fechar tudo, rodar de novo continua do
mesmo checkpoint (no mesmo dia). Em outro dia, use
`PRODESP_ROLEMEMBERS_SNAPSHOT_DATE=<data> /mnt/asper/scripts/run_full_load.sh`.

Pode ligar o pipeline (passos 4–6) com a carga ainda rodando. Se o disparo das
03:00 pegar a carga em andamento, ele vê o lock e sai sem fazer nada.

### 4. `pipelines.yml`

```yaml
- pipeline.id: prodesp-rolemembers-daily
  path.config: "/etc/logstash/pipelines/prodesp-rolemembers-daily.conf"
  queue.type: persisted
  pipeline.workers: 1   # worker tem estado
```

### 5. Unit systemd

Em `/usr/lib/systemd/system/logstash.service`, **sem hífen** (CLAUDE.md 3.10).
O `.env` do passo 1 já precisa existir:

```
EnvironmentFile=/etc/logstash/envio_prodesp_rolemembers.env
```

```bash
systemctl daemon-reload
```

### 6. Reiniciar e validar

Não reinicie com um backfill rodando em outro pipeline (3.11). Reiniciar
durante uma execução **deste** pipeline não perde nada: a próxima execução do
mesmo snapshot retoma do estado. Atenção: a do dia seguinte começa um
snapshot novo.

```bash
systemctl restart logstash
# 2-3 min de boot
curl -s --max-time 10 "http://127.0.0.1:9600/_node/stats/jvm,process,pipelines" | python3 /usr/local/bin/ls_health.py
```

Depois das 03:00:

```bash
set -a; . /etc/logstash/envio_prodesp_rolemembers.env; set +a   # so pra usar as vars no curl abaixo

# resumo de cada execucao
curl -s -u "$ELASTIC_USER:$ELASTIC_PASSWORD" "$ELASTIC_URL/logs-cyberark.prodesprolemembers-default/_search" \
  -H 'Content-Type: application/json' -d '{"size":10,"sort":[{"@timestamp":"desc"}],
  "query":{"term":{"doc_type":"run_summary"}}}'

# roles que nao fecharam (coletado < FullCount)
curl -s -u "$ELASTIC_USER:$ELASTIC_PASSWORD" "$ELASTIC_URL/logs-cyberark.prodesprolemembers-default/_search" \
  -H 'Content-Type: application/json' -d '{
  "size": 100, "_source": ["run_id","RoleName","status","expected_count","collected_count","attempts","error"],
  "query": {"bool": {"filter": [{"term": {"doc_type": "role_summary"}}, {"range": {"@timestamp": {"gte": "now-1d"}}}],
                     "must_not": [{"term": {"status": "ok"}}]}}}'
```

No servidor, o estado do dia mostra onde cada role parou:

```bash
cat /var/lib/logstash/prodesp-rolemembers/$(date +%F)/roles/*/meta.json | python3 -c 'import sys,json
for l in sys.stdin.read().replace("}{","}\n{").splitlines():
    m=json.loads(l)
    if m["status"]!="ok": print(m["RoleName"],m["status"],m.get("collected_count"),"/",m.get("expected_count"),m.get("error") or "")'
```

### Smoke test rápido (opcional)

Poucas roles, estado em `/tmp` para não mexer no real:

```bash
cd /mnt/asper/scripts
sudo -u logstash PRODESP_ROLEMEMBERS_STATE_DIR=/tmp/rm_state PRODESP_ROLEMEMBERS_ROLES_MAX_COUNT=5 \
  PRODESP_ROLEMEMBERS_USERS_MAX_PAGES=1 PRODESP_ROLEMEMBERS_USERS_PAGE_SIZE=1000 ./run_load_now.sh
```

## Comparar com o `cyberark-prodesp-directory`

Para o mesmo `snapshot_date`:

- **Membros por role.** Aqui é `terms(RoleName)` com `doc_type:member`. Lá é
  `terms(role_name)`. As roles com diferença grande são candidatas a explicar a
  perda (aninhadas, timeout do GetRoleMembers etc.).
- **Usuários distintos com role.** Aqui é `cardinality(Username)` com
  `doc_type:member and not RoleName:Everybody`. Lá é `cardinality(username)`
  com `role_id` existente.
- **Usuários sem role.** Aqui é `doc_type:user and has_role:false`. Lá é
  `role_id` nulo.

O prodesp-directory roda às 20h/23h e este às 03:00. O `snapshot_date` dos dois
é a data BRT do disparo, então compare o dia D daqui com o dia D-1 de lá
(algumas horas de diferença) ou rode um `run_load_now.sh` aqui no mesmo dia.
