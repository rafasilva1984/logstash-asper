# Deploy — pipeline `cyberark-apiprodesp-directory` (join users x roles)

> Clone do runbook do `cyberark-detran-directory`, adaptado para o tenant
> CyberArk **ApiProdesp** (`abb4725.id.cyberark.cloud`, app "ApiProdesp",
> `client_id=user_bi_elastic`). ⚠️ **NÃO é o mesmo tenant** do
> `cyberark-prodesp-directory` já existente (tenant `prodesp.id.cyberark.cloud`)
> — nomes parecidos, tenants diferentes, pedido explícito do usuário em
> 2026-09-13 para não confundir os dois.

## Contexto

- Usuário pediu um pipeline novo no mesmo padrão do
  `cyberark-detran-directory` (join `User` x `Role`, uma única data
  stream, um doc por par usuário+role).
- **2026-09-13**: probe inicial devolveu `FullCount: 0` em `User`/`Role`/
  `Event` neste tenant — só a tabela `Application` tinha 1 linha (a
  própria app OAuth, `State: "Active"`), confirmando que auth/conexão
  funcionavam mas sem dado nenhum de diretório. Hipótese registrada na
  época: tenant recém-provisionado, sem sync. Ficou pendente o usuário
  confirmar com quem administra o CyberArk.
- **2026-09-14**: usuário voltou dizendo que o time confirmou ser
  **permissão do usuário de serviço** (não falta de sync/provisionamento).
  Reprobado nesta sessão: dado real presente (`User`=7.016, `Role`=70,
  `Event` 24h=16.044) — pipeline desbloqueado.

## Probe manual (2026-09-14, desta máquina Windows via curl)

Mesma sequência do CLAUDE.md §8 ("probe manual"):

- Token OAuth2 obtido normalmente.
- `SELECT User.Username, User.ID AS UserId, User.Status AS UserStatus,
  User.LastLogin FROM User` — mesma forma de query do Detran, funciona
  sem erro. `FullCount: 7016`.
- `SELECT Role.ID, Role.Name FROM Role` — `FullCount: 70`.
- `POST /Roles/GetRoleMembers` testado em 3 roles (`Everybody` — 0
  membros, é role implícita/virtual, mesmo padrão visto no Detran;
  `sysadmin` — 23 membros, só `Type: "User"`; `PRODESP`, a maior role —
  4.886 membros).
- Somando `FullCount` de `GetRoleMembers` nas 70 roles: **9.706 pares
  usuário+role** no total. Top 3 roles por volume: `PRODESP` (4.886),
  `outsoucing - prod` (3.253), `Reembolso` (1.399); as demais têm poucas
  dezenas ou menos.
- **`GetRoleMembers` deste tenant também IGNORA `PageNumber`/`PageSize`**
  e devolve a role inteira numa única resposta, `hasMoreRows: false` já
  na primeira chamada (testado com `PageSize=1`, `500` e `2000` na role
  de 4.886 membros — sempre voltam os 4.886, mesmo tempo, ~11-14s cada).
  Mesmo comportamento do tenant Detran — **não** universal entre tenants
  CyberArk (não confiar sem probar de novo em tenant futuro), mas
  confirmado aqui **antes** de escrever o worker (evitou repetir o
  incidente de paginação infinita que aconteceu no Detran).
- Não observado nenhum membro `Type: "Role"` (role aninhada) nas roles
  sondadas — filtro por `Type` mantido no worker por segurança mesmo
  assim (mesmo padrão Detran/Prodesp).

## Como o join foi resolvido

Igual ao Detran: roles vêm de `Role` (dump rápido, dimensão pequena) +
`GetRoleMembers` chamado uma vez por role (paralelizável,
`APIPRODESP_ROLES_MAX_WORKERS=5`); depois pagina a tabela `User` inteira
e casa cada usuário com o dicionário `join_key -> [roles]`. Formato
achatado (um doc por par usuário+role, usuário sem role com
`role_id`/`role_name` nulos). Join key: `User.ID` (mais robusto que
`Username`).

`_id = sha256(snapshot_date + join_key + role_id)` — idempotente por
snapshot; rerun no mesmo dia vira `409` (dedup benigno). Sem checkpoint:
cada execução é um dump completo independente.

## Status em 2026-09-14 (fim de sessão) — já feito vs. o que falta

✅ **JÁ FEITO** (direto desta máquina local, sem tocar o servidor
`logstash-prod-01`):
- Probe completo contra o tenant real (confirma dado presente e schema).
- Worker escrito com `hasMoreRows` desde o início (sem precisar
  redescobrir o bug de paginação que o Detran encontrou em produção).
- **Smoke test pequeno** (`APIPRODESP_USERS_MAX_PAGES=1`,
  `APIPRODESP_ROLES_MAX_COUNT=5`): 200 docs, `last_login_ms` parseado
  corretamente do formato `/Date(<ms>)/`.
- **Carga completa local**: 26,9s, 7.016 usuários, 9.739 documentos, zero
  `ALERTA`/falha de role.
- **Bootstrap já rodado**: ILM policy (`logs-cyberark.apiprodespdirectory-ilm`,
  hot 7d → frozen em `found-snapshots`, sem delete), index template
  (`logs-cyberark.apiprodespdirectory-template`) e data stream
  `logs-cyberark.apiprodespdirectory-default` já existem no Elastic Cloud
  (todos `{"acknowledged":true}`). **Não precisa rodar de novo.**
- **Push funcional já rodado** (`functional_push.py`, sem passar pelo
  Logstash): 9.739 documentos criados, 0 duplicado, 0 falha.
- **Validação origem × Elastic já rodada** (`validate_cyberark_apiprodesp.sh`):
  usuários 7.016 origem / 7.034 Elastic (drift de poucos minutos entre a
  carga e a validação, esperado); pares usuário+role 9.706 origem / 9.679
  Elastic (diferença pequena, mesma explicação do Detran — roles
  aninhadas filtradas + timing). Números batem dentro do esperado.

🔲 **FALTA** (só pode ser feito no servidor `logstash-prod-01`):
- Passos 1-3 abaixo (copiar arquivos, criar `.env`, registrar
  `EnvironmentFile` + `daemon-reload`).
- Passo 6 (registrar no `pipelines.yml`).
- Passo 7 (reiniciar o Logstash e validar).
- **Passos 4 (bootstrap) e 5/teste funcional podem ser PULADOS** — já
  feitos com sucesso desta máquina. Rodar de novo no servidor é opcional
  (idempotente).

## Passo a passo (no servidor `logstash-prod-01`)

### 1. Copiar os arquivos para o servidor

```bash
scp pipelines/cyberark-apiprodesp-directory/cyberark_apiprodesp_worker.py      root@logstash-prod-01:/etc/logstash/pipelines/
scp pipelines/cyberark-apiprodesp-directory/cyberark-apiprodesp-directory.conf root@logstash-prod-01:/etc/logstash/pipelines/
scp pipelines/cyberark-apiprodesp-directory/bootstrap_cyberark_apiprodesp.sh   root@logstash-prod-01:/mnt/asper/scripts/
scp pipelines/cyberark-apiprodesp-directory/validate_cyberark_apiprodesp.sh    root@logstash-prod-01:/mnt/asper/scripts/
```

> `.conf` e worker `.py` em `/etc/logstash/pipelines/`; scripts
> auxiliares em `/mnt/asper/scripts/` (CLAUDE.md §1).

Validar sintaxe do worker:

```bash
python3 -c "import ast; ast.parse(open('/etc/logstash/pipelines/cyberark_apiprodesp_worker.py').read()); print('OK')"
```

### 2. Criar o `.env`

```bash
scp pipelines/cyberark-apiprodesp-directory/envio_cyberark_apiprodesp.env.example root@logstash-prod-01:/etc/logstash/envio_cyberark_apiprodesp.env
ssh root@logstash-prod-01 'chown logstash:logstash /etc/logstash/envio_cyberark_apiprodesp.env; chmod 600 /etc/logstash/envio_cyberark_apiprodesp.env'
```

### 3. Registrar o `EnvironmentFile` na unit systemd

Adicionar em `/usr/lib/systemd/system/logstash.service`:

```
EnvironmentFile=/etc/logstash/envio_cyberark_apiprodesp.env
```

**Sem hífen** (obrigatório — CLAUDE.md 3.10). Confirme que o arquivo já
existe (passo 2) antes de reiniciar, senão derruba TODOS os pipelines.

```bash
systemctl daemon-reload
```

### 4. Bootstrap do template/ILM — JÁ FEITO, pular

Já rodado com sucesso desta máquina em 2026-09-14 (ver "Status" acima).
Só rodar de novo se quiser confirmar (idempotente):

```bash
set -a; source /etc/logstash/envio_cyberark_apiprodesp.env; set +a
export ELASTIC_USERNAME="$ELASTIC_USER"   # o bootstrap espera ELASTIC_USERNAME
cd /mnt/asper/scripts
./bootstrap_cyberark_apiprodesp.sh
```

### 5. Carga inicial de teste / teste funcional — JÁ FEITO, pular

Já rodado com sucesso desta máquina em 2026-09-14: 9.739 documentos no
data stream de produção, `last_login_ms` confirmado preenchido para
usuários com login, join key (`User.ID`/`Guid`) confirmado funcionando
(usuários das roles processadas aparecem com `role_id`/`role_name`
preenchidos, não nulos). Opcional rodar no servidor só pra confirmar
acesso de rede do servidor ao tenant (auth/DNS/firewall podem diferir da
máquina local):

```bash
scp pipelines/cyberark-apiprodesp-directory/functional_push.py     root@logstash-prod-01:/mnt/asper/scripts/
scp pipelines/cyberark-apiprodesp-directory/run_functional_test.sh root@logstash-prod-01:/mnt/asper/scripts/
ssh root@logstash-prod-01 'chmod +x /mnt/asper/scripts/run_functional_test.sh /mnt/asper/scripts/functional_push.py'

set -a; source /etc/logstash/envio_cyberark_apiprodesp.env; set +a
cd /mnt/asper/scripts
./run_functional_test.sh   # carga completa, ~27s neste tenant
```

### 6. Registrar no `pipelines.yml`

```yaml
- pipeline.id: cyberark-apiprodesp-directory
  path.config: "/etc/logstash/pipelines/cyberark-apiprodesp-directory.conf"
  queue.type: persisted
  pipeline.workers: 1   # dump completo por execucao, sem necessidade de paralelismo aqui
```

### 7. Reiniciar e validar

```bash
systemctl restart logstash   # 2-3 min de boot, API 9600 nao responde nesse tempo
```

```bash
curl -s --max-time 10 "http://127.0.0.1:9600/_node/stats/jvm,process,pipelines" | python3 /usr/local/bin/ls_health.py
```

O pipeline só dispara no próximo horário do `schedule`
(`APIPRODESP_SCHEDULE`, default `0 20,23 * * *` hora local do servidor —
`timedatectl` já confirmado `America/Sao_Paulo` para os outros
pipelines desta plataforma). Pra validar sem esperar até a noite, force
um disparo real uma vez: setar temporariamente `APIPRODESP_SCHEDULE` no
`.env` para um horário próximo em hora local, `systemctl restart
logstash`, confirmar boot antes do horário alvo, conferir que os dados
chegaram, e devolver o `.env` para o valor de produção.

```bash
curl -s -u "$ELASTIC_USER:$ELASTIC_PASSWORD" "${ELASTIC_URL}/logs-cyberark.apiprodespdirectory-default/_count"
```

Já deve mostrar pelo menos os 9.739 documentos da carga local de
2026-09-14 (reruns no mesmo `snapshot_date` viram `409` benigno).

### 8. Validar origem × Elastic (sanity-check de volume)

```bash
set -a; source /etc/logstash/envio_cyberark_apiprodesp.env; set +a
cd /mnt/asper/scripts
./validate_cyberark_apiprodesp.sh                # snapshot de hoje (BRT)
./validate_cyberark_apiprodesp.sh 2026-09-14      # ou uma data especifica
```

## Handoff ao cliente

- Uma única data stream: `logs-cyberark.apiprodespdirectory-default`,
  join de `User` com `Role` (via `GetRoleMembers` por role) já feito na
  ingestão — um documento por par usuário+role, usuário sem role aparece
  com `role_id`/`role_name` nulos.
- Ingestão 2x/noite às 20h e 23h (horário de Brasília), snapshot
  completo — sem incremental, sem checkpoint, **~27s de duração** neste
  tenant (volume pequeno: 7.016 usuários, 70 roles). Histórico
  preservado dia a dia (ILM hot 7d → frozen, sem delete) para auditoria
  de "quem tinha qual role quando" (campo `snapshot_date` em cada doc).
- Pendências conhecidas (mesmo padrão da plataforma): credenciais em
  texto puro no `.env` (rotacionar quando for para produção definitiva);
  data view do Kibana (criar via `KIBANA_URL`/`KIBANA_DATA_VIEW_NAME` no
  bootstrap, ou manualmente).
