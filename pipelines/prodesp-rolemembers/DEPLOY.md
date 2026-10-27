# Deploy — pipeline `prodesp-rolemembers-daily` (CyberArk tenant Prodesp)

Runbook completo da versão atual (2026-10-27, duas vias por role: `mfa` +
`rm`). Serve tanto para a 1ª instalação quanto para **apagar o que já existe
e recriar tudo do zero**. Leia antes o `CLAUDE.md` da raiz (§3 e §10).

Nenhum passo pede `export` nem colar chave. O `envio_prodesp_rolemembers.env`
real desta pasta já vem preenchido (Elastic + client `elastic_bi_asper`), e
todos os scripts leem o `.env` sozinhos. Ele é gitignored e está só na
estação: copie com `scp`.

---

## 1. O que é

Porta o script do cliente que gerava `baserole_membros.csv` (`Username;RoleName`):
catálogo de roles e, **para cada role**, quem é membro. Além disso, faz o dump
da tabela `User` para incluir quem não tem role nenhuma.

É o **mesmo dado** do `cyberark-prodesp-directory` (quem tem qual role), mas por
**outra regra**. Aquele usa `POST /Roles/GetRoleMembers`, e percebemos perda
de dados lá. Este pipeline é **novo e isolado** e roda lado a lado para
comparar. O antigo não é alterado.

| Item | Valor |
|------|-------|
| pipeline.id | `prodesp-rolemembers-daily` (workers=1, tem estado) |
| Worker / conf | `prodesp_rolemembers_worker.py` / `prodesp-rolemembers-daily.conf` |
| Seed de roles pesadas | `prodesp_rolemembers_role_stats.seed.json` (ao lado do worker) |
| Codec | `json_lines` + heartbeat (CLAUDE.md 3.1/3.2) |
| Agendamento | `schedule` `0 3 * * *`: **03:00** hora local do servidor, 1x/dia |
| Data stream | `logs-cyberark.prodesprolemembers-default` |
| ILM | `logs-cyberark.prodesprolemembers-ilm`: hot 7d (rollover 7d) → frozen, **sem delete** |
| Template | `logs-cyberark.prodesprolemembers-template` |
| `.env` | `/etc/logstash/envio_prodesp_rolemembers.env` (prefixo `PRODESP_ROLEMEMBERS_`) |
| Estado/checkpoint | `/var/lib/logstash/prodesp-rolemembers/` (dono logstash) |
| Carga full | `/mnt/asper/scripts/run_full_load.sh` (background, fora do Logstash) |
| Log da carga full | `/var/log/logstash/prodesp-rolemembers-fullload.log` |

### Arquivos e destino no servidor

| Arquivo (repo) | Destino |
|----------------|---------|
| `prodesp_rolemembers_worker.py` | `/etc/logstash/pipelines/` |
| `prodesp-rolemembers-daily.conf` | `/etc/logstash/pipelines/` |
| `prodesp_rolemembers_role_stats.seed.json` | `/etc/logstash/pipelines/` |
| `envio_prodesp_rolemembers.env` (real, gitignored) | `/etc/logstash/envio_prodesp_rolemembers.env` |
| `bootstrap_prodesp_rolemembers.sh` | `/mnt/asper/scripts/` |
| `run_full_load.sh` | `/mnt/asper/scripts/` |
| `run_load_now.sh` | `/mnt/asper/scripts/run_load_now_rolemembers.sh` (nome já usado por outros pipelines) |
| `prodesp_rolemembers_push.py` | `/mnt/asper/scripts/` (os dois scripts acima procuram ele na mesma pasta) |

---

## 2. Como funciona

### Duas vias por role

Medido por probe em 2026-10-26/27:

- **`mfa`**: o relatório do cliente, `@/lib/reports/compliance/mfa.js` com
  `roleId`. Traz Username + colunas de MFA (`IdentityProfile`,
  `IdentityChallenge1/2`, `RadiusProfile`, `RadiusChallenge1/2`). O custo
  **cresce com o tamanho da role e não depende do PageSize**: o relatório
  calcula a role inteira antes de paginar. Com página de 10 linhas, a Balcao
  Unico passou de 420s. O servidor da CyberArk **corta qualquer chamada em
  ~300s** (HTTP 500, página HTML "Idaptive"), então role grande nunca sai por
  aqui, por mais retry ou timeout que se dê.
- **`rm`**: SQL na `RoleMember` **filtrada pela role**
  (`WHERE RoleMember.RoleID = '<id>'`), paginada. Balcao Unico 6.218 em 1s;
  SEDUC - Tutoria **190.688 em ~1,5 min** (páginas de 10.000 em 3–7s), com
  `FullCount` exato. Devolve o `MemberID` (= `User.ID`); o Username vem do
  dump de User. A tabela inteira, sem `WHERE`, trava (incidente #1 do
  prodesp-directory), mas filtrada por role responde.

Regra:
- **Role leve** → `mfa` (regra do cliente). No máximo 2 tentativas de 330s.
- **Role pesada** → direto pela `rm`. Pesada = FullCount ≥ 2000, chamada ≥ 60s
  ou já falhou, segundo o histórico em `/var/lib/logstash/prodesp-rolemembers/role_stats.json`.
  Na 1ª vez vale o seed medido no probe: 59 roles pesadas.
- **`mfa` falhou** → troca para `rm` **na hora**, na mesma passada.

Todo doc `member` leva `source_method` (`mfa`/`rm`). Todo `role_summary` leva
também `rolemember_count`, a contagem da RoleMember para aquela role, como
conferência cruzada com o que o `mfa.js` devolveu. Isso ajuda a investigar a
perda.

### Completude ("não deixar nada de fora")

1. **Nenhuma role é pulada**, inclusive `Everybody`. Ela é virtual (0 membros
   gravados; todo usuário é membro implícito) e não conta no
   `role_count`/`has_role` do doc `user`.
2. **A role só fecha quando coletado == FullCount** da via usada. Senão: retry
   por chamada com backoff exponencial (5 tentativas, 15s → 300s) e até 10
   passadas sobre as roles abertas. Se a paginação terminar e não bater (a
   lista mudou no meio), a role é refeita do zero. As linhas já vistas também
   são emitidas, porque são membros reais vistos no dia.
3. **Checkpoint por role/página**, guardando **as próprias linhas** em disco.
   O `exec` só entrega o stdout ao Logstash quando o processo termina
   (CLAUDE.md 3.11). Se a execução cair, o que ela coletou está no disco. A
   próxima execução do mesmo snapshot retoma das páginas que faltam e
   **reemite tudo**, e o `_id` determinístico transforma a repetição em 409.
   Se o PageSize mudar no `.env`, as páginas salvas com o tamanho antigo são
   descartadas e aquela coleta recomeça.
4. **Paralelismo**: leves 4 ao mesmo tempo, pesadas 1 por vez (filas
   simultâneas).
5. **Dump de User primeiro** (checkpoint por página), porque a via `rm` precisa
   dele. Membros via `rm` só são emitidos com o dump completo. Docs `user` só
   saem quando todas as roles fecharam. O `_id` é por dia, então um
   `role_count` parcial emitido antes ficaria gravado errado (a versão certa
   viraria 409).
6. **Log final e `role_summary`** acusam qualquer role com coletado < FullCount
   (`[fim] NAO FECHOU: ...`). Há também um `run_summary` por execução.

Cada execução para de buscar em 23h (`MAX_RUNTIME_MINUTES=1380`) e emite o que
tem. Duas execuções nunca rodam juntas: há um lock (`flock`) no diretório de
estado, e a segunda sai na hora. Estado com mais de 3 dias é apagado.

### Documentos gerados

| `doc_type` | Um por | `_id` | Campos principais |
|------------|--------|-------|-------------------|
| `member` | par (role, usuário) no dia | `member\|snapshot_date\|RoleId\|Username` | `RoleId`, `RoleName`, `Username`, `UserId`, `source_method`; só na via `mfa`: `IdentityProfile`, `IdentityChallenge1/2`, `RadiusProfile`, `RadiusChallenge1/2` |
| `user` | usuário da tabela User no dia | `user\|snapshot_date\|UserId` | `Username`, `UserId`, `UserStatus`, `LastLogin`, `role_count`, `has_role` |
| `role_summary` | role por **execução** | `summary\|run_id\|RoleId` | `status` (ok/incompleto/erro/pendente), `source_method`, `expected_count`, `collected_count`, `rolemember_count`, `mfa_full_count`, `rm_full_count`, `attempts`, `restarts`, `error`, `mfa_error` |
| `run_summary` | execução | `run\|run_id` | `roles_ok`, `roles_not_ok`, `roles_mfa`, `roles_rm`, `expected_count`, `collected_count`, `members_emitted`, `users_emitted`, `users_without_role`, `users_complete`, `duration_s` |

Todos levam `snapshot_date` (data BRT do disparo). `@timestamp` = momento do
disparo. Membro via `rm` cujo `MemberID` não existe na tabela User sai com
`Username: null` e `UserId` preenchido.

---

## 3. Passo a passo (do zero)

Todos os comandos dos passos 3.2 em diante rodam **no servidor**
(`logstash-prod-01`), como root.

### 3.1. Copiar os arquivos (da estação)

```bash
cd pipelines/prodesp-rolemembers

scp prodesp_rolemembers_worker.py prodesp-rolemembers-daily.conf prodesp_rolemembers_role_stats.seed.json \
    root@logstash-prod-01:/etc/logstash/pipelines/

scp bootstrap_prodesp_rolemembers.sh run_full_load.sh prodesp_rolemembers_push.py \
    root@logstash-prod-01:/mnt/asper/scripts/
scp run_load_now.sh root@logstash-prod-01:/mnt/asper/scripts/run_load_now_rolemembers.sh

scp envio_prodesp_rolemembers.env \
    root@logstash-prod-01:/etc/logstash/envio_prodesp_rolemembers.env    # o REAL, ja com credenciais
```

### 3.2. Parar o que estiver rodando

Pare qualquer carga full ou execução do worker:

```bash
/mnt/asper/scripts/run_full_load.sh --stop 2>/dev/null
pkill -f run_full_load.sh; pkill -f prodesp_rolemembers_worker.py
pgrep -fa "prodesp_rolemembers|run_full_load" || echo "nada rodando"
```

Se o pipeline já estava registrado no Logstash (`pipelines.yml`), comente as
linhas dele por enquanto para ele não disparar no meio da reinstalação. Ele
volta no passo 3.9. Não precisa reiniciar o Logstash agora: o disparo é só às
03:00, e o `--stop` acima já matou uma execução em curso.

### 3.3. Apagar o estado antigo no servidor

O estado de versões anteriores tem o layout antigo e tentativas de `mfa` que
não fecham. Apague tudo, inclusive o histórico de roles pesadas (o seed novo
assume) e o log da carga anterior:

```bash
rm -rf /var/lib/logstash/prodesp-rolemembers
rm -f  /var/log/logstash/prodesp-rolemembers-fullload.log

mkdir -p /var/lib/logstash/prodesp-rolemembers
chown logstash:logstash /var/lib/logstash/prodesp-rolemembers
```

### 3.4. Apagar e recriar no Elastic (data stream, template, ILM)

Hoje o data stream só tem dados de teste (≈670 docs `member` de 2026-10-26).
Para começar limpo, apague na ordem **data stream → template → policy**. O
template não sai enquanto o data stream existir, e a policy não sai enquanto
estiver em uso.

⚠️ Apagar o data stream apaga **todos** os documentos dele, sem volta. Faça só
enquanto ele tiver apenas teste, ou se quiser mesmo zerar o histórico.

```bash
set -a; . /etc/logstash/envio_prodesp_rolemembers.env; set +a   # so pra ter as vars no curl
A=(-u "$ELASTIC_USER:$ELASTIC_PASSWORD")

# o que existe hoje
curl -s "${A[@]}" "$ELASTIC_URL/logs-cyberark.prodesprolemembers-default/_count"; echo

# apagar
curl -s "${A[@]}" -X DELETE "$ELASTIC_URL/_data_stream/logs-cyberark.prodesprolemembers-default"; echo
curl -s "${A[@]}" -X DELETE "$ELASTIC_URL/_index_template/logs-cyberark.prodesprolemembers-template"; echo
curl -s "${A[@]}" -X DELETE "$ELASTIC_URL/_ilm/policy/logs-cyberark.prodesprolemembers-ilm"; echo
# cada um deve responder {"acknowledged":true}; 404 = ja nao existia (ok)
```

Recriar (idempotente; lê o `.env` sozinho):

```bash
chmod +x /mnt/asper/scripts/bootstrap_prodesp_rolemembers.sh
/mnt/asper/scripts/bootstrap_prodesp_rolemembers.sh
```

A saída termina com `template= logs-cyberark.prodesprolemembers-template |
ilm_policy= logs-cyberark.prodesprolemembers-ilm | backing= [...]` e
`bootstrap concluido.`

> Se o data stream ainda tiver docs ao rodar o `DELETE`, o Elastic apaga
> mesmo assim. Se aparecer `index_template ... is in use`, o `DELETE` do data
> stream falhou: confira o nome e repita.

### 3.5. Permissões e conferência

```bash
python3 -c "import ast; ast.parse(open('/etc/logstash/pipelines/prodesp_rolemembers_worker.py').read()); print('OK')"
python3 -c "import json; print(len(json.load(open('/etc/logstash/pipelines/prodesp_rolemembers_role_stats.seed.json'))), 'roles no seed')"

chmod +x /mnt/asper/scripts/run_full_load.sh /mnt/asper/scripts/run_load_now_rolemembers.sh
chown logstash:logstash /etc/logstash/envio_prodesp_rolemembers.env
chmod 600 /etc/logstash/envio_prodesp_rolemembers.env

grep -E 'SCHEDULE|RM_PAGE_SIZE|MFA_TIMEOUT|ROLE_STATS_SEED|STATE_DIR' /etc/logstash/envio_prodesp_rolemembers.env
# esperado: SCHEDULE="0 3 * * *", RM_PAGE_SIZE=10000, MFA_TIMEOUT_SECONDS=330,
#           ROLE_STATS_SEED=/etc/logstash/pipelines/prodesp_rolemembers_role_stats.seed.json,
#           STATE_DIR=/var/lib/logstash/prodesp-rolemembers
```

### 3.6. Smoke test (opcional, ~2 min)

Poucas roles, estado em `/tmp` (não mexe no real). Não sobe nada pro Elastic
além dos membros dessas roles:

```bash
cd /mnt/asper/scripts
sudo -u logstash PRODESP_ROLEMEMBERS_STATE_DIR=/tmp/rm_smoke \
  PRODESP_ROLEMEMBERS_ROLES_FILTER="Acesso API,Balcao Unico,Everybody" \
  PRODESP_ROLEMEMBERS_USERS_MAX_PAGES=1 PRODESP_ROLEMEMBERS_USERS_PAGE_SIZE=1000 \
  ./run_load_now_rolemembers.sh
rm -rf /tmp/rm_smoke
```

Esperado no `[fim]`: as 3 roles `ok`, `via={'mfa': 1, 'rm': 2}`. Os membros via
`rm` saem como "ADIADOS" (o teste limita o dump de User), o que é o
comportamento correto.

### 3.7. Carga FULL (deixar rodando)

```bash
/mnt/asper/scripts/run_full_load.sh
```

Volta na hora e segue em background como `logstash`, mesmo fechando o SSH.
Faz: dump de User (~1,27M, ~16 min) → todas as roles (leves via `mfa`, pesadas
via `rm`) → push direto pro Elastic. Repete, retomando do checkpoint, até
**todas as roles fecharem** e o dump de User terminar (até 5 rodadas). O
`snapshot_date` é fixado no início.

```bash
tail -f /var/log/logstash/prodesp-rolemembers-fullload.log   # progresso role a role
/mnt/asper/scripts/run_full_load.sh --status                  # rodando? ultimo resumo
/mnt/asper/scripts/run_full_load.sh --stop                    # para; o coletado fica no checkpoint
```

No fim aparece `CONCLUIDO` ou a lista `NAO FECHOU: <role>=<status>
coletado/FullCount`. Rodar de novo no mesmo dia continua do checkpoint. Em
outro dia, continue o mesmo snapshot com
`PRODESP_ROLEMEMBERS_SNAPSHOT_DATE=<AAAA-MM-DD> /mnt/asper/scripts/run_full_load.sh`.

Pode seguir para os passos 3.8–3.10 com a carga rodando. Se o disparo das 03:00
pegar a carga em andamento, ele vê o lock e sai sem fazer nada.

### 3.8. Unit systemd

Em `/usr/lib/systemd/system/logstash.service`, **sem hífen** (CLAUDE.md 3.10).
O `.env` do passo 3.1 já precisa existir, senão o Logstash inteiro não sobe:

```
EnvironmentFile=/etc/logstash/envio_prodesp_rolemembers.env
```

```bash
grep -n prodesp_rolemembers /usr/lib/systemd/system/logstash.service   # nao duplicar a linha
ls -l /etc/logstash/envio_prodesp_rolemembers.env
systemctl daemon-reload
```

### 3.9. `pipelines.yml`

```yaml
- pipeline.id: prodesp-rolemembers-daily
  path.config: "/etc/logstash/pipelines/prodesp-rolemembers-daily.conf"
  queue.type: persisted
  pipeline.workers: 1   # worker tem estado
```

(Se você comentou no passo 3.2, descomente.)

### 3.10. Reiniciar e validar

Não reinicie com um backfill rodando em outro pipeline (3.11). Reiniciar com a
carga full deste pipeline rodando não tem problema, porque ela roda fora do
Logstash.

```bash
systemctl restart logstash
# 2-3 min de boot
curl -s --max-time 10 "http://127.0.0.1:9600/_node/stats/jvm,process,pipelines" | python3 /usr/local/bin/ls_health.py
grep -i rolemembers /var/log/logstash/logstash-plain.log | tail
```

---

## 4. Validação

Depois da carga full, e depois de cada 03:00:

```bash
set -a; . /etc/logstash/envio_prodesp_rolemembers.env; set +a
A=(-u "$ELASTIC_USER:$ELASTIC_PASSWORD"); DS=logs-cyberark.prodesprolemembers-default

# resumo de cada execucao
curl -s "${A[@]}" "$ELASTIC_URL/$DS/_search" -H 'Content-Type: application/json' -d '{
  "size":5,"sort":[{"@timestamp":"desc"}],"query":{"term":{"doc_type":"run_summary"}}}' | python3 -m json.tool

# roles que nao fecharam (coletado < FullCount)
curl -s "${A[@]}" "$ELASTIC_URL/$DS/_search" -H 'Content-Type: application/json' -d '{
  "size":100,"_source":["run_id","RoleName","status","source_method","expected_count","collected_count","error"],
  "query":{"bool":{"filter":[{"term":{"doc_type":"role_summary"}},{"range":{"@timestamp":{"gte":"now-1d"}}}],
                   "must_not":[{"term":{"status":"ok"}}]}}}' | python3 -m json.tool

# conferencia cruzada: roles via mfa cuja contagem difere da RoleMember
curl -s "${A[@]}" "$ELASTIC_URL/$DS/_search" -H 'Content-Type: application/json' -d '{
  "size":300,"_source":["RoleName","source_method","collected_count","rolemember_count"],
  "query":{"bool":{"filter":[{"term":{"doc_type":"role_summary"}},{"range":{"@timestamp":{"gte":"now-1d"}}}]}}}' \
  | python3 -c 'import sys,json
for h in json.load(sys.stdin)["hits"]["hits"]:
    s=h["_source"]
    if s.get("rolemember_count") is not None and s.get("collected_count")!=s.get("rolemember_count"):
        print(s["RoleName"],s["source_method"],"coletado=",s["collected_count"],"RoleMember=",s["rolemember_count"])'
```

Estado do dia, no servidor (onde cada role parou):

```bash
cat /var/lib/logstash/prodesp-rolemembers/$(date +%F)/roles/*/meta.json | python3 -c 'import sys,json
for l in sys.stdin.read().replace("}{","}\n{").splitlines():
    m=json.loads(l)
    if m["status"]!="ok": print(m["RoleName"],m.get("method"),m["status"],m.get("collected_count"),"/",m.get("expected_count"),m.get("error") or "")'
```

---

## 5. Comparar com o `cyberark-prodesp-directory`

Para o mesmo `snapshot_date`:

- **Membros por role.** Aqui é `terms(RoleName)` com `doc_type:member`. Lá é
  `terms(role_name)`. As roles com diferença grande são candidatas a explicar a
  perda.
- **Usuários distintos com role.** Aqui é `cardinality(Username)` com
  `doc_type:member and not RoleName:Everybody`. Lá é `cardinality(username)`
  com `role_id` existente.
- **Usuários sem role.** Aqui é `doc_type:user and has_role:false`. Lá é
  `role_id` nulo.

O prodesp-directory roda às 20h/23h e este às 03:00. O `snapshot_date` de cada
um é a data BRT do disparo: compare o dia D daqui com o D-1 de lá.

---

## 6. Parâmetros principais (`.env`)

| Variável | Valor | Para quê |
|----------|-------|----------|
| `PRODESP_ROLEMEMBERS_SCHEDULE` | `"0 3 * * *"` | disparo diário (hora local) |
| `PRODESP_ROLEMEMBERS_MFA_PAGE_SIZE` / `_MFA_TIMEOUT_SECONDS` / `_MFA_MAX_ATTEMPTS` | 5000 / 330 / 2 | via `mfa` |
| `PRODESP_ROLEMEMBERS_RM_PAGE_SIZE` | 10000 | via `rm` |
| `PRODESP_ROLEMEMBERS_TIMEOUT_SECONDS` | 330 | queries SQL (o servidor corta em ~300s) |
| `PRODESP_ROLEMEMBERS_LIGHT_WORKERS` / `_HEAVY_WORKERS` | 4 / 1 | paralelismo |
| `PRODESP_ROLEMEMBERS_HEAVY_MIN_COUNT` / `_HEAVY_MIN_SECONDS` | 2000 / 60 | o que é role pesada |
| `PRODESP_ROLEMEMBERS_MAX_RETRIES` / `_MAX_PASSES` | 5 / 10 | retry por chamada / passadas |
| `PRODESP_ROLEMEMBERS_MAX_RUNTIME_MINUTES` | 1380 | teto de uma execução (23h) |
| `PRODESP_ROLEMEMBERS_USERS_PAGE_SIZE` | 50000 | dump de User |
| `PRODESP_ROLEMEMBERS_STATE_DIR` / `_KEEP_DAYS` | `/var/lib/logstash/prodesp-rolemembers` / 3 | checkpoint |
| `PRODESP_ROLEMEMBERS_ROLE_STATS_SEED` | `/etc/logstash/pipelines/prodesp_rolemembers_role_stats.seed.json` | roles pesadas na 1ª vez |

Só para teste (não usar em produção): `ROLES_FILTER`, `ROLES_MAX_COUNT`,
`SKIP_ROLES`, `USERS_MAX_PAGES`, `FORCE_METHOD` (`mfa`/`rm`), `SNAPSHOT_DATE`.

---

## 7. Histórico

- **2026-10-26**: 1ª versão (só `mfa.js`, timeout de 1h). Bootstrap no Elastic.
  Probe: 224 roles; 166 respondem ao `mfa.js` em até 60s (11.407 membros) e 58
  estouram.
- **2026-10-27**: a 1ª carga full "travou" na fila pesada (Balcao Unico com
  HTTP 500 a cada ~300s, ~28 min por role só para falhar). Diagnóstico acima
  (§2): corte do servidor em ~300s, custo do `mfa.js` independente do
  PageSize, `GetRoleMembers` sem paginação e também estourando. Achada a
  `RoleMember` filtrada por role. Nasceu a versão de duas vias. Teste: Acesso
  API (mfa) 15/15; Balcao Unico (rm) 6.218/6.218; SEDUC - Tutoria (rm)
  190.688/190.688; Everybody (rm) 0/0; `rolemember_count` bateu nas 4. Com o dump de User completo
  (**1.269.509 usuários em ~16 min**, 26 páginas de 50.000), os 196.921
  membros saíram 100% com Username resolvido e sem `_id` repetido. A proteção
  de PageSize descartou corretamente páginas salvas com outro tamanho.
