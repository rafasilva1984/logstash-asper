# Deploy — pipeline `cyberark-detran-directory` (join users x roles)

> ⚠️ **Este arquivo é um clone do runbook do `cyberark-prodesp-directory`,
> só com texto renomeado (prodesp→detran) em 2026-09-08.** O tenant de
> origem (`abb4724.id.cyberark.cloud`, `/OAuth2/Token/PainelDetran`,
> `user_bi_elastic`) é REAL e confirmado — é o mesmo já usado pelo
> pipeline `usersselo-snapshot` (CLAUDE.md §8). Mas os **incidentes,
> tempos de resposta e comportamento de `RoleMember`/`GetRoleMembers`
> narrados abaixo foram medidos no tenant Prodesp** (backend Postgres/
> Npgsql diferente) — ainda **NÃO validados** para este join no tenant
> Detran/abb4724. Refazer os probes manuais (§8 do CLAUDE.md) antes de
> assumir que os mesmos timeouts/incidentes se aplicam aqui.

Runbook para colocar em produção o pipeline novo a partir dos scripts em
`novopipe/` (protótipo de export CSV). Leia o `CLAUDE.md` da raiz antes,
principalmente §3 e §10 — este runbook segue o mesmo padrão dos demais.

## Contexto (registrado pelo cliente)

- `main.txt` era só o script de controle (limpa CSV antigo, chama as
  duas extrações em sequência).
- `query_roles.txt` (RoleMember join User join Role) foi a base inicial,
  mas usuários **sem nenhuma role** não apareciam nela.
- `query_users.txt` (dump completo da tabela `User`) foi criado pelo
  Danilo Brito depois, exatamente para cobrir esses usuários sem role.
- A quebra em arquivos de 500 mil linhas era só para não estourar o CSV
  gerado — **não existe mais**: no Elastic é um documento por linha, sem
  limite de arquivo.
- **Pedido do cliente**: os dados precisam ficar numa **única data
  stream**, como um "join" de users com roles — não duas streams
  separadas.

## ⚠️ Incidente do primeiro deploy (2026-08-24) — leia antes de mexer em roles

A carga inicial travou mais de 40 min sem indexar nada. Diagnóstico
completo (probes manuais, na ordem que foram feitos):

1. Rede/DNS/TLS até `abb4724.id.cyberark.cloud` — **ok**, instantâneo.
2. `/OAuth2/Token/PainelDetran` (auth) — **ok**, responde em <1s.
3. `/Redrock/query` com a query original de roles (JOIN
   `RoleMember`/`User`/`Role` via `split_part`/`regexp_replace`) —
   **timeout total**, 0 bytes, mesmo pedindo 1 linha só.
4. Isolando: `SELECT RoleMember.ID FROM RoleMember` **sem nenhum JOIN**
   — **também trava**. Não é o JOIN, é a tabela `RoleMember` que não é
   consultável em bulk nesse tenant via `/Redrock/query`.
5. `SELECT User.Username FROM User` (sem join) — responde, mas devagar:
   ~15,8s pra 1 linha, ~57,4s pra 50.000 linhas. Custo real por linha
   (~0,8ms/linha) além de um fixo de ~15s — provavelmente `ORDER BY`
   sem índice + cálculo de `FullCount` a cada chamada. `Role` (tabela
   pequena, ~211 roles) responde rápido (~0,3s) por esse mesmo endpoint.
6. Achada a saída: `POST /Roles/GetRoleMembers` (endpoint REST dedicado
   do Centrify/CyberArk Identity, fora do `/Redrock/query`) devolve os
   membros de uma role em <1s, mesmo pra role com dezenas de membros.
   Testado também com uma role potencialmente enorme (`Everybody`) —
   voltou vazia e rápida (é role implícita/virtual, sem membership
   real armazenada).

**Conclusão / mudança de arquitetura**: roles não vêm mais de
`RoleMember` via SQL. Vêm de `Role` (dump rápido, dimensão pequena) +
`GetRoleMembers` chamado **uma vez por role** (rápido, paralelizável).
É bem provável que o script CSV original (`query_roles.txt`) sempre
tenha sofrido disso silenciosamente: quando uma página falhava nas 3
tentativas, a função retornava lista vazia, que o loop interpretava
como "fim dos dados" em vez de erro — os CSVs de roles gerados até
hoje podem estar incompletos sem ninguém ter percebido.

## Como o join foi resolvido

Elasticsearch não faz JOIN em tempo de consulta como um banco
relacional — para uma única data stream, o join acontece **no worker
Python**, antes de indexar: primeiro coleta as ~211 roles e, pra cada
uma, busca seus membros via `GetRoleMembers` (ver incidente acima),
montando um dicionário `user_id -> [roles]`; depois pagina a tabela
`User` inteira e, para cada usuário, casa com esse dicionário.

Formato escolhido (confirmado com o cliente): **achatado, um documento
por par (usuário, role)** — como um `LEFT JOIN User -> RoleMember` de
SQL:

```json
{ "username": "joao", "user_id": "123", "last_login_ms": ..., "beneficiario": "S",
  "role_id": "10", "role_name": "Admin" }
{ "username": "joao", "user_id": "123", "last_login_ms": ..., "beneficiario": "S",
  "role_id": "20", "role_name": "Operador" }
{ "username": "maria", "user_id": "456", "last_login_ms": ..., "beneficiario": "N",
  "role_id": null, "role_name": null }
```

Usuário com N roles gera N documentos (os campos do usuário se repetem
em cada um); usuário sem nenhuma role gera 1 documento com
`role_id`/`role_name` nulos. Essa forma foi escolhida em vez de um
documento por usuário com array `roles` (nested) porque funciona direto
no Discover/Lens do Kibana — agregação padrão, sem precisar de nested
queries. Contrapartida aceita: dado do usuário duplicado por role
(volume baixo, não é problema aqui).

Join key: `User.ID`, comparado com o campo `Guid` que o `GetRoleMembers`
devolve pra cada membro do tipo `User` (mais robusto que `Username`) —
por isso a query de `User` foi estendida para trazer também
`User.ID`/`User.Status`. **Não validado por probe ainda que `Guid` ==
`User.ID` em valor** (só que os dois têm cara de UUID Centrify) —
conferir no smoke test (passo 5): pegar um usuário que apareceu como
membro de alguma role no probe (ex.: `danilo.brito@asper.tec.br` em
`sysadmin`) e verificar se o `user_id` dele no NDJSON de `users` bate
com o `Guid` visto no probe. Se não bater, o join falha silenciosamente
(todo mundo sai com `role_id: null`) — nesse caso ajustar `join_key()`
no worker pra usar `Username`/`Name` em vez de `User.ID`/`Guid`.

## O que mudou em relação ao protótipo

- Dois scripts (`query_users.txt`, `query_roles.txt`) viram **um único
  worker** (`cyberark_detran_worker.py`) que coleta roles, coleta
  users, faz o join em memória e emite o NDJSON já unificado — mesmo
  worker, **um único pipeline Logstash, uma única data stream**:
  `cyberark-detran-directory` → `logs-cyberark.detrandirectory-default`.
- Sem CSV, sem `ThreadPoolExecutor` escrevendo em arquivo — cada linha
  vira NDJSON no stdout, consumido pelo Logstash (`codec => json_lines`
  + heartbeat, volume grande — CLAUDE.md 3.1/3.2).
- **Sem checkpoint.** As duas queries de origem são dumps completos (sem
  filtro de tempo — `SELECT * FROM User`, `SELECT * FROM RoleMember JOIN
  ...`), não há janela pra retomar. Cada execução noturna é um
  "snapshot" completo independente.
- **Idempotência por snapshot, não por evento**: o `_id` é
  `sha256(snapshot_date + user_id + role_id)`, onde `snapshot_date` é a
  data BRT do disparo. Rodar de novo no mesmo dia (ex.: reteste manual)
  vira `409` (dedup benigno); a próxima noite gera um snapshot novo — o
  histórico completo fica preservado (ILM sem fase de delete), dá pra
  auditar "quem tinha qual role em que dia".
- **Coleta de roles totalmente redesenhada** (ver incidente acima): o
  paralelismo por grupo de username (`0`-`9`) do script original não
  existe mais — a tabela `RoleMember` (base da query original) não
  funciona em bulk nesse tenant. Agora é paralelismo por **role**
  (`DETRAN_ROLES_MAX_WORKERS`, 57 roles no total neste tenant), cada uma
  via `GetRoleMembers`.
- `DETRAN_USERS_PAGE_SIZE` default mantido em 50000 (herdado do Prodesp,
  onde amortizava um custo fixo alto por chamada) — nesse tenant o volume
  é bem menor (19.338 linhas de `User`, dump completo em ~4-5s) e o
  worker completo (users + roles) leva **~90s**, confirmado por 2 smoke
  tests reais em 2026-09-08 (ver seção de incidente abaixo).

## Decisões já confirmadas com o cliente

1. **Fuso do cron — CORRIGIDO em 2026-08-24 (não confiar no que diz mais
   abaixo neste histórico, só nesta entrada).** A suposição original
   era "servidor roda em UTC, `0 23 * * *` = 23:00 UTC = 20:00 BRT" —
   **errada**, e só descoberta durante a carga de teste de hoje, que
   ficou horas sem disparar. `timedatectl` no servidor confirma
   `Time zone: America/Sao_Paulo (-03, -0300)` — o SO (e por
   consequência o `rufus-scheduler` do Logstash) roda em **hora
   local**, não UTC. O `schedule` correto é `0 20 * * *` **direto**,
   sem nenhuma conversão — já é 20:00 BRT. `.conf` e `.env.example`
   corrigidos para esse valor. Se algum dia o servidor mudar de
   timezone, reconferir com `timedatectl` antes de mexer no cron de
   novo — não assumir.
2. **Formato de `User.LastLogin`.** Confirmado: valida durante o smoke
   test (passo 5) e segue. O worker já guarda os dois campos —
   `last_login_raw` (bruto) e `last_login_ms` (parseado no formato
   `/Date(<ms>)/`, `null` se não bater) — então nada se perde enquanto
   isso é conferido. **Checklist do smoke test**: depois do passo 5,
   olhar `head -3 /tmp/dbg.ndjson` e confirmar se `last_login_ms` veio
   preenchido; se estiver sempre `null`, olhar `last_login_raw` pra
   descobrir o formato real e ajustar `parse_cyberark_date_ms` no worker
   antes do passo 6 (registrar no `pipelines.yml`).
3. **Credenciais.** Confirmado: reaproveitar `DETRAN_CLIENT_ID` /
   `DETRAN_CLIENT_SECRET` do tenant Detran (mesmas do `usersselo-snapshot`,
   já no `.env.example`). Nenhuma conta de serviço nova a criar.
4. **Data stream única (este documento).** Confirmado: join achatado
   (um doc por par usuário+role) numa única data stream, em vez de duas
   separadas.

## ⚠️ Incidente #2 (2026-08-24/25) — zero dado subiu desde o primeiro deploy

Depois de resolvido o incidente #1 (roles via `GetRoleMembers`) e o
pipeline registrado, **nenhum documento chegou ao Elastic** nos dias
seguintes. Diagnóstico:

- `ps aux | grep cyberark_detran_worker.py` — processo nunca rodou.
- `grep -i "cyberark.detran\|cyberark_detran" /var/log/logstash/logstash-plain.log`
  — nenhuma linha, nunca.
- `grep DETRAN_SCHEDULE /etc/logstash/envio_cyberark_detran.env` —
  devolveu `DETRAN_SCHEDULE="45 0 * * */'"`. Cron **corrompido** (sobrou
  `*/'` no final) — provavelmente resto de uma edição manual malfeita
  durante o teste do passo 7 (setar horário próximo pra forçar disparo,
  depois devolver pro valor de produção) que nunca foi corrigida de
  volta. Cron inválido → o `schedule` do input `exec` nunca casa com
  nada → job nunca dispara → silêncio total, sem erro óbvio nos logs.

**Causa raiz: erro humano de edição do `.env` em produção, não bug no
worker/`.conf`/arquitetura.** O `.env.example` do repo sempre esteve
correto.

**Correção aplicada:**
```bash
sed -i 's/^DETRAN_SCHEDULE=.*/DETRAN_SCHEDULE="0 20,23 * * *"/' /etc/logstash/envio_cyberark_detran.env
grep DETRAN_SCHEDULE /etc/logstash/envio_cyberark_detran.env   # SEMPRE conferir apos editar
systemctl restart logstash
```

**Mudança de desenho (rede de segurança, não ingestão contínua):**
inicialmente cogitou-se redesenhar pra ingestão contínua (cadências
separadas users/roles + cache) pra evitar depender de um horário fixo.
**Descartado**: o requisito real do cliente é só "dado pronto e correto
toda manhã" — ingestão contínua traria custo extra no backend (que já
travou uma vez, ver incidente #1) sem necessidade, e mudar a
granularidade do `_id` de dia pra execução implicaria revisitar a
decisão #4 já combinada com o cliente (doc por dia). Em vez disso: o
`schedule` passou de `0 20 * * *` (1x) para `0 20,23 * * *` (2x na
mesma noite, lista no campo hora — sintaxe cron padrão, mesmo `exec`,
sem mudança de arquitetura). Se a execução das 20h falhar por qualquer
motivo (rede, auth, `.env` errado como aconteceu aqui), a das 23h ainda
cobre a manhã seguinte; `_id` continua por `snapshot_date`, então a
2ª rodada só é custo redundante pra quem não mudou (409 benigno) e
cobertura real pra quem mudou entre as duas.

**Pendência aberta**: confirmar no smoke test seguinte que os dois
horários (20h e 23h) realmente disparam e que o `_id` dedup funciona
como esperado entre eles (ver checklist do passo 8, comparar contagem
apos as duas execucoes da mesma noite).

## ⚠️ Incidente #3 (2026-08-25) — role anômala trava a coleta inteira em silêncio

Durante o primeiro teste funcional de carga completa (`run_functional_test.sh`
sem limites), a fase de roles (`fetch_all_roles`) ficou **30+ minutos sem
nenhuma linha nova no log**, mas o processo continuava vivo:

- `ps -o pid,etimes,stat,%cpu,cmd` — processo ativo, ~35min decorridos, CPU
  baixa (típico de espera de rede, não de loop CPU-bound).
- `ss -tnp | grep :443` — **5 conexões HTTPS estabelecidas** com o CyberArk,
  uma por worker (`DETRAN_ROLES_MAX_WORKERS=5`) — confirma que estava
  mesmo tentando trabalhar, não travado num deadlock de lock Python.
- `grep -c "falhou apos"` no log — só 2 ocorrências, ambas nos primeiros
  ~2 minutos da execução. Nada de novo depois disso.

**Diagnóstico**: `fetch_all_roles()` só loga "coleta finalizada" depois que
**todas** as futures do `ThreadPoolExecutor` terminam — uma única role
"anômala" (paginação que nunca esvazia abaixo de `ROLES_PAGE_SIZE`, sem
nunca dar timeout porque cada página individual respondia dentro dos 30s)
prende uma das 5 threads pra sempre, e o restante do lote de roles (as
outras ~200 já processadas rápido) fica todo esperando essa travar
`as_completed()` nunca fechar o `with ThreadPoolExecutor`. Sem timeout de
página que dispare, sem erro, sem log — silêncio total. Mesma família do
incidente #1 (esse tenant tem roles/tabelas individuais instáveis no
backend), só que dessa vez sem nenhum sintoma de erro pra apontar a causa.

**Correção aplicada no worker**:
- `DETRAN_ROLES_MAX_PAGES_PER_ROLE` (default `20`) — teto duro de páginas
  por role em `fetch_role_members`; ao atingir, loga um `ALERTA` com o
  `role_id` e quantos membros já tinha coletado, e para SÓ aquela role
  (as outras continuam normalmente). Nunca deve ser atingido numa role
  normal (dezenas de membros cabem numa página só de 2000).
- `DETRAN_ROLE_MEMBERS_TIMEOUT_SECONDS` (default `15`, era `30`
  hardcoded) — encurta o custo de retry de uma role realmente lenta.

**Pendência**: identificar qual role específica causou isso (o worker
antigo não logava o `role_id` até travar de vez — com o teto novo, a
próxima carga completa vai apontar exatamente qual, no log, se acontecer
de novo). Considerar reportar ao time do CyberArk/Centrify se for sempre
a mesma role.

**Ajuste em 2026-08-26 — timeout subido de 15s pra 25s**: rodando
`validate_cyberark_detran.sh` (que soma `FullCount` de `GetRoleMembers`
por role, mesmo endpoint do worker, mas com timeout **hardcoded em 30s**
no próprio script — não lê `DETRAN_ROLE_MEMBERS_TIMEOUT_SECONDS`) contra
o snapshot de 2026-08-25, **18 das 211 roles (~8,5%) deram timeout mesmo
com 30s**. Confirma que a instabilidade é genuína do backend (mesma
família dos Incidentes #1/#3), não foi introduzida pela redução pra 15s —
mas nos 15s do worker essas mesmas roles ficavam de fora do snapshot toda
noite (retry não ajuda quando o problema é latência real de ~15-25s, não
falha transiente). `DETRAN_ROLE_MEMBERS_TIMEOUT_SECONDS` subido pra
`25` (default no worker e no `.env.example`) — meio-termo: ainda bem
abaixo do que a validação mostrou ser necessário pra sondagem completa
(30s), mas cobre a faixa 15-25s que estava sendo cortada sem necessidade.
Continuar monitorando o log (`[roles][<id>] falhou: ...`) pra ver se ainda
sobra alguma role consistentemente fora mesmo com 25s.

## Ponto resolvido pelo probe de 2026-09-08 (tenant Detran, abb4724)

- **`User.Beneficiario_` NÃO existe no tenant Detran** — query falha com
  `column User.Beneficiario_ does not exist`. Campo específico do Prodesp,
  removido do `USERS_SCRIPT`, do doc emitido e do mapping do bootstrap
  (era um resquício do clone). Não há campo equivalente confirmado neste
  tenant — se o cliente pedir algo parecido, é preciso probe novo.
- **Roles grandes e lentas existem também aqui** (não é só o Prodesp):
  probe encontrou 57 roles no total; a maioria pequena e rápida (<10
  membros, <1s), mas duas roles ("Adesao - eCRV", 6230 membros, ~14s
  isolada; "Adesao - eCNH", 13329 membros, ~26s isolada) demoraram o
  suficiente pra estourar o timeout herdado de 25s quando rodando em
  paralelo com outras 4 roles (`DETRAN_ROLES_MAX_WORKERS=5`). Nenhuma
  role travou em paginação infinita (todas responderam com `hasMoreRows:
  false` numa página só, mesmo com `PageSize=5000`) — não é o mesmo bug
  do Incidente #3 do Prodesp, é só volume real. **Ação**: subir
  `DETRAN_ROLE_MEMBERS_TIMEOUT_SECONDS` pra 45s (margem sobre os ~26s
  observados isolado, considerando contenção de rede sob concorrência) —
  já aplicado no worker/`.env.example`.

## ⚠️ Incidente do 1º smoke test (2026-09-08) — GetRoleMembers ignora PageSize/PageNumber

`run_functional_test.sh` completo (sem limites), 1ª vez contra o tenant
Detran real: **458s**, bem acima do esperado pelo probe (<1min). Causa:
`GetRoleMembers` deste tenant **ignora `PageNumber`/`PageSize` e sempre
devolve a role INTEIRA numa única resposta**, com `hasMoreRows: false` já
na primeira chamada — mesmo comportamento que o probe já tinha visto, mas
o código do worker não confiava nesse campo: o loop comparava
`len(linhas) < ROLES_PAGE_SIZE` (2000) pra decidir parar. Como o backend
sempre devolve tudo de uma vez, `len(linhas)` nunca fica menor que
`ROLES_PAGE_SIZE` pra role com MAIS de 2000 membros — 3 das 57 roles
("Detran - PAM" 2456, "Adesao - eCRV" 6230, "Adesao - eCNH" 13329) caíram
nisso, gerando **20 chamadas redundantes por role** (mesma role inteira
devolvida de novo a cada "página") até bater no teto de segurança
`DETRAN_ROLES_MAX_PAGES_PER_ROLE` (20) — 3 linhas de log `ALERTA`
(49120/124600/266580 "membros" contados = tamanho real × 20, exato).

**Corrigido**: `post_with_retry` agora devolve o `Result` bruto da API
(não só a lista de `Results`), e `request_role_members` extrai também
`hasMoreRows`, usado pelo loop pra decidir parar em vez de comparar
tamanhos. **2º smoke test, já com o fix**: **91,8s** (roles ~34s + users
~49s), zero `ALERTA`, resultado idêntico ao 1º (mesmos 30.157 documentos,
mesmos `_doc_id` — confirma que o *join* já estava correto, só desperdiçava
tempo) e 100% `409` (dedup benigno, mesma `snapshot_date` do dia).

**Duração real confirmada por 2 execuções completas**: ~90s — nada perto
dos 20-25min do Prodesp. Considerar agendamento mais frequente que
`DETRAN_SCHEDULE` herdado (20h/23h) se o cliente quiser dado mais fresco.

**Achado secundário (não bloqueia)**: 3 usuários apareceram como membro de
alguma role mas ausentes no dump de `User` (prováveis contas removidas/
inativas) — logado como aviso, doc não emitido pra eles (comportamento
correto: role órfã, sem par válido pra indexar).

## Status em 2026-09-08 (fim de sessão) — o que já está feito vs. o que falta

✅ **JÁ FEITO** (direto desta máquina local, sem tocar o servidor
`logstash-prod-01`):
- Probe manual completo contra o tenant Detran (credenciais, `Beneficiario_`
  removido, volumes reais medidos).
- **Bootstrap já rodado** (passo 4 abaixo): ILM policy, index template e
  data stream `logs-cyberark.detrandirectory-default` já existem no
  Elastic Cloud (status GREEN, confirmado). **Não precisa rodar de novo.**
- **Bug de paginação do `GetRoleMembers` encontrado e corrigido** no
  worker (ver incidente do 1º smoke test acima).
- **2 smoke tests completos já rodados com sucesso** contra o Elastic
  real (via `functional_push.py`, sem passar pelo Logstash): 30.157
  documentos já estão no data stream de produção, dedup (`409`) validado
  entre as duas rodadas, ~90s por execução completa.

🔲 **FALTA** (só pode ser feito no servidor `logstash-prod-01`, amanhã):
- Passos 1, 2, 3 (copiar arquivos, criar `.env`, registrar
  `EnvironmentFile` + `daemon-reload`).
- Passo 6 (registrar no `pipelines.yml`).
- Passo 7 (reiniciar o Logstash e validar).
- Passo 8 (rodar `validate_cyberark_detran.sh` no servidor, opcional já
  que a validação foi feita daqui, mas bom pra confirmar acesso de rede
  do servidor ao tenant).
- **Passo 4 (bootstrap) e o smoke test completo (passo 5) podem ser
  PULADOS** — já foram feitos com sucesso desta máquina. Rodar de novo no
  servidor é opcional (idempotente, não quebra nada, mas é redundante).

## Passo a passo (no servidor `logstash-prod-01`)

### 1. Copiar os arquivos para o servidor

```bash
scp pipelines/cyberark-detran-directory/cyberark_detran_worker.py      root@logstash-prod-01:/etc/logstash/pipelines/
scp pipelines/cyberark-detran-directory/cyberark-detran-directory.conf root@logstash-prod-01:/etc/logstash/pipelines/
scp pipelines/cyberark-detran-directory/bootstrap_cyberark_detran.sh   root@logstash-prod-01:/mnt/asper/scripts/
scp pipelines/cyberark-detran-directory/validate_cyberark_detran.sh    root@logstash-prod-01:/mnt/asper/scripts/
```

> `.conf` e worker `.py` em `/etc/logstash/pipelines/`; scripts
> auxiliares em `/mnt/asper/scripts/` (CLAUDE.md §1).

Validar sintaxe do worker:

```bash
python3 -c "import ast; ast.parse(open('/etc/logstash/pipelines/cyberark_detran_worker.py').read()); print('OK')"
```

### 2. Criar o `.env`

```bash
scp pipelines/cyberark-detran-directory/envio_cyberark_detran.env.example root@logstash-prod-01:/etc/logstash/envio_cyberark_detran.env
ssh root@logstash-prod-01 'chown logstash:logstash /etc/logstash/envio_cyberark_detran.env; chmod 600 /etc/logstash/envio_cyberark_detran.env'
```

### 3. Registrar o `EnvironmentFile` na unit systemd

Adicionar em `/usr/lib/systemd/system/logstash.service`:

```
EnvironmentFile=/etc/logstash/envio_cyberark_detran.env
```

**Sem hífen** (obrigatório — CLAUDE.md 3.10). Confirme que o arquivo já
existe (passo 2) antes de reiniciar, senão derruba TODOS os pipelines.

```bash
systemctl daemon-reload
```

### 4. Bootstrap do template/ILM (cria a data stream)

```bash
set -a; source /etc/logstash/envio_cyberark_detran.env; set +a
export ELASTIC_USERNAME="$ELASTIC_USER"   # o bootstrap espera ELASTIC_USERNAME

cd /mnt/asper/scripts
./bootstrap_cyberark_detran.sh
```

Cria: ILM policy (hot `${DETRAN_ILM_HOT_MAX_AGE:-7d}` → frozen em
`found-snapshots`, sem delete — mesmo padrão do `usersselo`, dataset de
snapshot/auditoria) + index template + a data stream
`logs-cyberark.detrandirectory-default`. Rodar **antes** de o pipeline
produzir dados (senão os docs falham na escrita, ver CLAUDE.md §7).

### 5. Carga inicial de teste (smoke test, sem envolver o Logstash)

Antes de confiar no schedule noturno, rodar o worker isolado com volume
reduzido pra validar rápido (auth, join, formato do `_doc_id`):

```bash
set -a; source /etc/logstash/envio_cyberark_detran.env; set +a

# reduz users a 1 pagina pequena e roles a poucas roles, so pra validar o pipeline ponta-a-ponta rapido
DETRAN_USERS_PAGE_SIZE=200 DETRAN_USERS_MAX_PAGES=1 DETRAN_ROLES_MAX_COUNT=5 \
  sudo -E -u logstash /usr/bin/python3 /etc/logstash/pipelines/cyberark_detran_worker.py \
  2>/tmp/dbg.log 1>/tmp/dbg.ndjson

wc -l < /tmp/dbg.ndjson
cat /tmp/dbg.log
head -3 /tmp/dbg.ndjson    # conferir last_login_raw / last_login_ms e role_id/role_name (nulo ou preenchido)
```

Checklist antes de seguir pro passo 6:
- `.ndjson` tem linhas de dado + a linha de `heartbeat`.
- `last_login_ms` veio preenchido (ver ponto 2 da seção de decisões
  acima) — se sempre `null`, ajustar `parse_cyberark_date_ms`.
- **Confirmar o join key funcionando**: com `DETRAN_ROLES_MAX_COUNT=5`
  só 5 roles são processadas, então é provável que NENHUM usuário da
  amostra pequena de `DETRAN_USERS_MAX_PAGES=1` bata com role nenhuma
  — isso sozinho não prova nada. Rode uma vez à parte, sem os limites,
  filtrando por um usuário específico que você já viu como membro de
  uma role no probe manual (ex.: procurar `danilo.brito` no
  `/tmp/dbg.ndjson` de uma rodada com `DETRAN_ROLES_MAX_COUNT=0` e
  `DETRAN_USERS_MAX_PAGES=0`, ou simplesmente `grep` o usuário depois
  da carga real) e conferir se ele aparece com `role_id`/`role_name`
  preenchidos, não `null`. Se o join key (`User.ID` vs `Guid`, ver seção
  acima) estiver errado, TODO MUNDO sai com `role_id: null` mesmo tendo
  role — esse é o jeito de pegar isso antes de confiar no snapshot.

Se tudo bater, siga para a seção **"Teste funcional imediato"** abaixo
para validar a carga completa contra o Elastic real, ou direto para o
passo 6 se preferir confiar só no `schedule`.

> Atualizado após o incidente #2: a orientação antiga aqui era "não faça
> a carga completa manualmente, espere o `schedule`" — foi exatamente
> confiar no `schedule` sem validar de ponta a ponta que permitiu o
> `.env` corrompido passar despercebido por dias. Agora o caminho
> recomendado é validar com carga completa manual (seção abaixo) ANTES
> de confiar em qualquer agendamento.

## Teste funcional imediato (sem depender do schedule)

Pensado pra ter certeza de que o processo inteiro funciona — auth, join,
escrita no Elastic — **agora**, sem esperar o horário noturno e sem
reiniciar o Logstash. Usa `functional_push.py`, que replica exatamente a
lógica do `filter`/`output` do `.conf` (monta `@timestamp` a partir de
`snapshot_ms`, remove `_doc_id`/`snapshot_ms`, `action=create` na data
stream) mas manda pro Elasticsearch direto via `_bulk`, sem passar pelo
Logstash. **Pré-requisito**: passo 4 (bootstrap — ILM/template/data
stream) já executado, senão os docs falham na escrita.

`functional_push.py` e `run_functional_test.sh` precisam ficar **no
mesmo diretório** — o segundo procura o primeiro ao lado de si mesmo
(`$SCRIPT_DIR/functional_push.py`). Os dois são scripts auxiliares de
teste (não fazem parte do pipeline em produção), então os dois vão para
`/mnt/asper/scripts/`, **não** para `/etc/logstash/pipelines/`:

```bash
scp pipelines/cyberark-detran-directory/functional_push.py     root@logstash-prod-01:/mnt/asper/scripts/
scp pipelines/cyberark-detran-directory/run_functional_test.sh root@logstash-prod-01:/mnt/asper/scripts/
ssh root@logstash-prod-01 'chmod +x /mnt/asper/scripts/run_functional_test.sh /mnt/asper/scripts/functional_push.py'
```

No servidor:

```bash
set -a; source /etc/logstash/envio_cyberark_detran.env; set +a
cd /mnt/asper/scripts

# NOTA: já rodado com sucesso 2x desta máquina local em 2026-09-08 (ver
# "Status em 2026-09-08" no topo deste documento) -- rodar aqui no servidor
# e opcional, so pra confirmar que ELE TAMBEM tem acesso de rede ao tenant
# (auth/DNS/firewall podem diferir da maquina local).

# opcional: smoke rapido primeiro (poucos minutos), pra validar auth/join/escrita
# IMPORTANTE: limitar DETRAN_USERS_PAGE_SIZE tambem -- so DETRAN_USERS_MAX_PAGES=1
# ainda busca 1 pagina de ATE 50000 linhas reais (default de producao), o que nao
# e mais um smoke test rapido.
DETRAN_USERS_PAGE_SIZE=200 DETRAN_USERS_MAX_PAGES=1 DETRAN_ROLES_MAX_COUNT=5 ./run_functional_test.sh

# carga completa (~90s neste tenant, confirmado por 2 rodadas reais) -- essa e a que da certeza de verdade
./run_functional_test.sh
```

O script roda o worker, envia o NDJSON pro Elastic e ao final mostra: o
resumo do push (`created`/`duplicate_409`/`failed`), uma amostra das 3
primeiras linhas do NDJSON (conferir `last_login_ms` preenchido e
`role_id`/`role_name` quando aplicável) e a contagem atual no índice.
`failed > 0` no resumo = script sai com código de erro; olhar o
`*.push.log` da rodada pra ver o motivo de cada erro.

Depois, rode o `validate_cyberark_detran.sh` (passo 8) pra conferir
origem × Elastic com números reais. Só depois de bater os dois, decida
sobre o `schedule` (passo 7) — nada aqui deixa nada pendente pra noite.

### 6. Registrar no `pipelines.yml`

```yaml
- pipeline.id: cyberark-detran-directory
  path.config: "/etc/logstash/pipelines/cyberark-detran-directory.conf"
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

O pipeline só dispara no próximo horário do `schedule` (20:00 **hora
local do servidor** — ver correção do fuso na seção de decisões acima,
`timedatectl` confirma `America/Sao_Paulo`, então `0 20 * * *` já é
direto, sem conversão pra UTC). Pra validar sem esperar até a noite,
force um disparo real uma vez: setar temporariamente `DETRAN_SCHEDULE`
no `.env` para um horário próximo **em hora local** (ex.: daqui a 10-15
min, dá margem pro boot do restart), `systemctl restart logstash`,
confirmar com `systemctl status logstash | grep -E "Active|since"` que
já subiu **antes** do horário alvo, conferir que os dados chegaram, e
depois **devolver o `.env` para `0 20 * * *`** e reiniciar de novo.

**Tempo esperado da carga completa: ~90 segundos** neste tenant
(confirmado por 2 execuções reais completas em 2026-09-08 — bem diferente
do Prodesp, ver incidentes acima). Mesmo assim, como o input `exec` só
entrega o stdout quando o processo termina (CLAUDE.md 3.11), nada aparece
no Elastic até o worker acabar — acompanhe com `tail -f` no log
(`journalctl -u logstash`) se quiser ver o progresso antes disso.

```bash
curl -s -u "$ELASTIC_USER:$ELASTIC_PASSWORD" "${ELASTIC_URL}/logs-cyberark.detrandirectory-default/_count"
```

### 8. Validar origem × Elastic (sanity-check de volume)

```bash
set -a; source /etc/logstash/envio_cyberark_detran.env; set +a
cd /mnt/asper/scripts
./validate_cyberark_detran.sh                # snapshot de hoje (BRT)
./validate_cyberark_detran.sh 2026-08-23      # ou uma data especifica
```

Compara, para o `snapshot_date`: total de usuários, pares usuário+role
(docs com `role_id` preenchido), e docs sem role (`role_id` nulo) — cada
um contra o `COUNT(*)` correspondente na origem.

## Handoff ao cliente

- Uma única data stream: `logs-cyberark.detrandirectory-default`, join
  de `User` com `Role` (via `GetRoleMembers` por role — não mais
  `RoleMember`, ver incidente acima) já feito na ingestão — um
  documento por par usuário+role, usuário sem role aparece com
  `role_id`/`role_name` nulos (nada fica de fora, resolvendo o gap que
  motivou a criação da query de Users).
- Ingestão 2x/noite às 20h e 23h (horário de Brasília, herdado do
  Prodesp), snapshot completo — sem incremental, sem checkpoint, **~90s
  de duração** neste tenant (volume bem menor que o Prodesp — considerar
  agendamento mais frequente, já que o custo é baixo). Histórico
  preservado dia a dia (ILM hot 7d → frozen, sem delete) pra auditoria
  de "quem tinha qual role quando" (campo `snapshot_date` em cada doc).
- Data view no Kibana: criado automaticamente pelo bootstrap se
  `KIBANA_URL` + `KIBANA_DATA_VIEW_NAME` estiverem setados no `.env`;
  senão criar manualmente apontando pra essa data stream.
- Pendências conhecidas (mesmo padrão da plataforma): credenciais em
  texto puro no `.env` (rotacionar quando for para produção definitiva);
  validar formato real de `LastLogin` com uma amostra (ponto 2 acima).
