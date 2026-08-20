# CLAUDE.md — Plataforma de Ingestão Logstash → Elasticsearch (Asper / CyberArk)

> Documento de contexto para o Claude Code operar neste servidor sem repetir erros
> já resolvidos. Leia **inteiro** antes de criar ou alterar qualquer pipeline.
> Cada regra aqui foi paga com horas de debug em produção.

---

## 1. Infraestrutura

| Item | Valor |
|------|-------|
| Host | `ip-172-19-36-183` (node Logstash `logstash-prod-01`) |
| SO | RHEL 10 |
| Recursos | 32 vCPU, 64 GB RAM |
| Logstash | 8.19.x |
| Java | 21 |
| Python | 3.12 — binário `/usr/bin/python3` |
| Heap Logstash | **Fixo em 20 GB** (`-Xms20g -Xmx20g`) → uso real ~17%, folgado. Restart é LENTO (2-3 min de boot; API 9600 não responde durante o boot — sempre usar `curl --max-time`). |

### Elasticsearch (destino, Elastic Cloud)
- URL: `https://dados-pam.es.us-east-2.aws.elastic-cloud.com:9243`
- Usuário: `rafael.silva@asper.tec.br`
- Senha: `Casa@@4321`  ⚠️ **texto puro — candidata a rotação**
- Repositório de snapshot: `found-snapshots` (usado pelas fases frozen do ILM)
- Kibana: provavelmente `https://dados-pam.kb.us-east-2.aws.elastic-cloud.com:9243`
  (mesmo domínio trocando `.es.` por `.kb.`) — **confirmar no console do Elastic Cloud**.

### Sem proxy no ambiente do serviço.

### Convenção de diretórios no servidor
- **`.conf` e workers `.py`**: todos em `/etc/logstash/pipelines/` (não usar `conf.d/` —
  é a pasta real usada pela instalação, apesar de exemplos genéricos por aí mencionarem
  `conf.d`). Confirme sempre em `path.config` das entradas do `pipelines.yml`.
- **Scripts auxiliares** (`bootstrap_*.sh` e demais scripts de suporte, exceto o
  `ls_health.py` — esse fica em `/usr/local/bin/`, ver §9): `/mnt/asper/scripts/`.

---

## 2. Arquitetura geral dos pipelines

Padrão único da plataforma, repetido para cada fonte:

```
Logstash (input exec, orquestra)
   → chama worker Python (executa: auth, query, parse, imprime NDJSON no stdout)
   → filtro Logstash (parse do NDJSON, monta _id, limpa campos)
   → output elasticsearch (action=create, _id determinístico)
   → data stream com ILM (hot → frozen, sem delete)
```

Princípios:
- **Logstash orquestra, Python executa.** O worker faz todo o trabalho pesado
  (autenticação, consulta à API/DB, parsing) e emite **uma linha JSON por documento**
  (NDJSON) no **stdout**. Logs de diagnóstico do worker vão para **stderr** (nunca stdout —
  poluiria o NDJSON que o Logstash consome).
- **`action => create` + `_id` determinístico** → idempotência. Reprocessar um evento
  gera o mesmo `_id`, e o Elastic rejeita com 409 (dedup benigno). **409 no log é normal**
  em sobreposições de janela/overlap; não é erro.
- **ILM**: fase hot (rollover) → fase frozen (searchable_snapshot em `found-snapshots`),
  **sem fase de delete** (retenção para auditoria/compliance).

---

## 3. ⚠️ ARMADILHAS CRÍTICAS (bugs já sofridos — NÃO REPETIR)

Estas são as lições mais caras da plataforma. Cada uma custou horas.

### 3.1. `codec => json_lines` crasha com stdout vazio
- **Sintoma**: `ClassCastException: RubyNil cannot be cast to RubyString` em
  `BufferedTokenizerExt.extract`, em loop, saturando o nó.
- **Causa**: quando o worker passa um ciclo **sem emitir nada** no stdout, o codec
  `json_lines` recebe `nil` e estoura.
- **Duas soluções válidas, escolha por volume:**
  - **Saída PEQUENA (poucas linhas/ciclo)**: usar `codec => plain` no input + fazer o
    parse no filtro (drop vazio → split → json). Ver §4.1.
  - **Saída GRANDE (dumps, backfills, centenas de milhares de linhas)**: usar
    `codec => json_lines` no input **+ heartbeat obrigatório no worker** (ver 3.2).
    NÃO usar plain+split para saída grande (ver 3.3).

### 3.2. Heartbeat obrigatório (para pipelines com json_lines)
- O worker **sempre** emite ao menos uma linha ao final, mesmo sem dados:
  ```python
  sys.stdout.write('{"heartbeat":"<nome_pipeline>"}\n')
  sys.stdout.flush()
  ```
- Essa linha não tem `_doc_id` e é **descartada pelo filtro** (`if ![_doc_id] { drop {} }`).
- Garante stdout nunca-vazio → o `json_lines` nunca crasha. É o que permite usar
  json_lines com segurança para saídas grandes.

### 3.3. `codec => plain` + split NÃO escala para lotes grandes
- **Sintoma**: worker gera 300k+ documentos, mas `in=2, filtered=2, out=0` no pipeline —
  tudo dropado; índice não cresce.
- **Causa**: com `plain`, o stdout inteiro vira **UM único evento gigante**. A persisted
  queue tem limite de **64 MB por página** — um evento de 150 MB nunca passa; e mesmo que
  passasse, o parse falha.
- **Regra**: lote grande → `json_lines` no input (parseia linha-a-linha no decode,
  gerando N eventos pequenos). Ver 3.1.

### 3.4. `terminator => "\n"` no filtro split é interpretado LITERALMENTE
- **Sintoma**: silencioso e traiçoeiro — worker entrega 330k docs, `in=710 filtered=709
  out=0`, nada chega ao Elastic, sem nenhum erro no log.
- **Causa**: no Logstash, `"\n"` em config **não** é quebra de linha (a menos que
  `config.support_escapes: true`); é tratado como os caracteres literais `\` + `n`. O split
  não encontra, devolve o blob inteiro como um evento, que o filtro dropa.
- **Regra**: **NUNCA** especificar `terminator => "\n"`. O default do split JÁ é quebra de
  linha real. Simplesmente omita o `terminator`.

### 3.5. Timezone hardcoded em query desloca a janela (bug do `-03:00`)
- **Sintoma**: backfill funciona, mas o realtime "para" num horário e não avança; queries
  de janela estreita retornam **zero** enquanto a origem tem dados.
- **Causa**: função que formatava datas para a query da CyberArk anexava ` -03:00` **fixo**
  a um `datetime` que já estava em **UTC**. O CyberArk lia "14:00 UTC" como "14:00 -03:00"
  (= 17:00 UTC), deslocando a janela 3h para o futuro, para o vazio. No backfill (janela de
  1 dia) o overlap mascarava; no realtime (janela de minutos) zerava.
- **Regra**: trabalhar SEMPRE em UTC e formatar em UTC, sem sufixo de timezone hardcoded:
  ```python
  def to_cyberark_date(dt):
      return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
  ```
  Formato `YYYY-MM-DD HH:MM:SS` (ISO simples) é o que funciona no `Datefunc(...)`.

### 3.6. Janelas de query grandes derrubam o backend da CyberArk
- **Sintoma**: query de dezenas de dias falha consistentemente com
  `Exception while reading from stream` / `NpgsqlException`. Query de 1 dia funciona.
- **Causa**: o Postgres por trás da CyberArk (via Npgsql) não aguenta materializar/ordenar
  janelas grandes.
- **Regra**: **NENHUMA janela de query pode passar de 1 dia.** Backfill e realtime devem
  fatiar em pedaços ≤ 1 dia. Ver a varredura unificada em §5.

### 3.7. Retry obrigatório para falhas transientes da CyberArk
- O `Exception while reading from stream` também ocorre **esporadicamente** mesmo em
  janelas pequenas. Sem retry, uma falha isolada aborta o backfill e **perde a fatia**
  (foi o que "matou" workers antes — não era OOM).
- **Regra**: worker deve ter retry com backoff. Distinguir transiente (stream/npgsql/
  timeout/deadlock → retenta) de permanente (sintaxe/permissão → aborta sem retentar).
  Ao esgotar retries, **não avançar o checkpoint** além do consolidado; a próxima execução
  retoma o ponto.

### 3.8. `_id` deve refletir unicidade REAL do evento
- **Sintoma**: índice satura num número fixo (ex.: travou em 95.135) e não cresce, apesar
  do worker ler milhões; tudo vira 409.
- **Causa**: usávamos `_id = Uuid`, mas **o `Uuid` da CyberArk se repete entre dias
  diferentes** (não é único por evento). Eventos distintos colapsavam no mesmo `_id`.
- **Regra**: compor o `_id` com campos que garantam unicidade real. Para o login Prodesp:
  ```python
  base = f"{uuid}|{WhenLogged}|{login_name}|{from_ip}"
  doc_id = hashlib.sha256(base.encode("utf-8")).hexdigest()
  ```
  **Nunca assumir** que um campo "id" da origem é único no tempo — validar antes.

### 3.9. `@timestamp` = tempo do EVENTO, não da ingestão
- O filtro `date` mapeia o campo de tempo real do evento (ex.: `WhenLogged`) para
  `@timestamp`. Isso faz o backfill aparecer distribuído no histórico do Discover, não
  amontoado em "hoje". Sempre setar via filtro `date`.

### 3.10. `EnvironmentFile=` sem hífen é OBRIGATÓRIO
- Na unit do systemd, `EnvironmentFile=/etc/logstash/xxx.env` (sem hífen) faz o arquivo ser
  **obrigatório**: se faltar, o Logstash **não sobe** — e derruba TODOS os pipelines, não só
  o que falta. (Com hífen `-/etc/...` seria opcional.)
- **Regra**: ao adicionar um pipeline, garantir que o `.env` dele existe ANTES de reiniciar.
  Após editar a unit, `systemctl daemon-reload` (senão vem aviso "changed on disk" e a versão
  antiga fica carregada).

### 3.11. Nunca reiniciar o Logstash durante um backfill
- O input `exec` só entrega o stdout ao pipeline **quando o processo Python termina**. Um
  restart no meio de um backfill longo mata o worker e **perde o stdout já produzido**
  (`IOError: stream closed`). Com o design em fatias, a perda máxima é uma fatia (o checkpoint
  retoma), mas ainda assim: **evite reiniciar durante backfill**.

### 3.12. API que só devolve sessão FECHADA filtrando pelo INÍCIO perde sessão longa em janela estreita
- **Fonte**: pipeline `sws-recordings` (reescrita, Alero, `/sws/recordings`).
- **Sintoma**: sessões longas (dezenas de minutos/horas) somem da ingestão
  quase-tempo-real, mesmo com checkpoint+overlap funcionando corretamente.
- **Causa**: a API filtra `recordings` pelo horário de **início** da sessão
  e só devolve sessão **já fechada**. Uma sessão que começou fora da janela
  realtime (porque ainda estava aberta quando a janela passou por ali) nunca
  mais vai bater no filtro de início nas janelas seguintes — passou pra
  frente e não volta.
- **Regra**: rodar um **segundo pipeline de reconciliação**, agendado (ex.:
  1x/dia), revarrendo uma janela larga e fixa (ex.: últimas 48h) sem
  depender de checkpoint. Como o `_id` é determinístico e o output usa
  `action => create`, tudo que já foi indexado pelo realtime vira `409`
  (dedup benigno); só as sessões que faltaram entram de fato. Ver
  `pipelines/sws-recordings/` (worker com `--mode realtime|reconcile`).
  Esse padrão vale para qualquer fonte futura com a mesma característica
  (filtro por início + só retorna fechado).

---

## 4. Padrões de `.conf`

### 4.1. Padrão PLAIN (saída pequena — sgp, sws)
Para pipelines cujo worker emite poucas linhas por ciclo.

```
input {
  exec {
    id => "<nome>_exec"
    command => "/usr/bin/python3 /etc/logstash/pipelines/<worker>.py"
    interval => 15
    codec => "plain"
  }
}
filter {
  if ![message] or [message] =~ /^\s*$/ { drop { } }   # ciclo vazio → evita crash
  split { id => "<nome>_split" field => "message" }     # SEM terminator (ver 3.4)
  if [message] =~ /^\s*$/ { drop { } }
  json { id => "<nome>_json" source => "message" }
  if ![_doc_id] { drop { } }                            # descarta linhas sem doc (heartbeat/log)
  mutate { add_field => { "[@metadata][doc_id]" => "%{[_doc_id]}" } }
  mutate { remove_field => ["_doc_id","command","host","@version","process","message"] }
}
output {
  if [@metadata][doc_id] {
    elasticsearch {
      hosts => ["${ELASTIC_URL}"]
      user => "${ELASTIC_USER}"
      password => "${ELASTIC_PASSWORD}"
      index => "${<VAR>_DATA_STREAM:<data-stream-default>}"
      action => "create"
      document_id => "%{[@metadata][doc_id]}"
      data_stream => false
      manage_template => false
    }
  }
}
```

### 4.2. Padrão JSON_LINES (saída grande — usersselo, loginprodesp)
Para dumps de tabela / backfills. Input com `json_lines`; worker COM heartbeat (3.2).

```
input {
  exec {
    id => "<nome>_exec"
    command => "/usr/bin/python3 /etc/logstash/pipelines/<worker>.py"
    interval => 60                # OU schedule => "0 6,18 * * *" para dumps agendados
    codec => "json_lines"
  }
}
filter {
  if ![_doc_id] { drop { } }      # descarta o heartbeat e linhas inválidas
  date {                          # @timestamp = tempo do evento (ver 3.9)
    match => ["<CampoTempo>", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "ISO8601"]
    target => "@timestamp"
    tag_on_failure => ["_dateparsefailure_<campo>"]
  }
  mutate { add_field => { "[@metadata][doc_id]" => "%{[_doc_id]}" } }
  mutate { remove_field => ["_doc_id","command","host","@version","process","message"] }
}
output { ... igual ao 4.1 ... }
```

Observações:
- `data_stream => false` + `manage_template => false` + `action => create` +
  `document_id` explícito: é assim que escrevemos em data stream com `_id` controlado.
- O `index =>` aponta para o nome do data stream; o template (criado no bootstrap) casa o
  padrão e aplica ILM/mappings.

---

## 5. Worker Python — estrutura de referência

Componentes que um worker de "eventos com janela" (tipo loginprodesp) deve ter:

1. **Config no topo**: `URL_TOKEN`, `URL_QUERY`, `CLIENT_ID`, `CLIENT_SECRET`, `PAGE_SIZE`,
   e via `os.environ` (com defaults): `BACKFILL_DAYS`, `SLICE_DAYS`, `OVERLAP_MINUTES`,
   `MAX_RETRIES`, `RETRY_BACKOFF`, `CHECKPOINT_FILE`.
2. **`gerar_token()`**: POST OAuth2 client_credentials, scope=all. Renova em 401.
3. **`to_cyberark_date(dt)`**: UTC, formato `YYYY-MM-DD HH:MM:SS` (ver 3.5). SEM `-03:00`.
4. **`converter_data(valor)`**: `/Date(ms)/` → datetime UTC.
5. **Checkpoint persistente**: lê/grava `CHECKPOINT_FILE` (escrita atômica `.tmp`+rename,
   monotônica — nunca retrocede). Diretório: `/var/lib/logstash/<pipeline>/` (dono
   `logstash:logstash`).
6. **`buscar_intervalo(inicio, fim, headers)`** com:
   - paginação (PageNumber/PageSize), acumulando até página < PAGE_SIZE;
   - **retry com backoff** (3.7): em success=false transiente → espera e retenta até
     MAX_RETRIES; permanente → aborta; retorna flag `ok` (3-tupla:
     `total, max_whenlogged, ok`);
   - monta o `_id` composto (3.8) e o doc; emite NDJSON no stdout.
7. **Varredura UNIFICADA e fatiada** (backfill == realtime, ver 3.6):
   - se sem checkpoint → início = `agora - BACKFILL_DAYS`; senão → início =
     `checkpoint - OVERLAP_MINUTES`.
   - varre em pedaços de 1 dia (00:00→23:59, último pedaço limitado a `agora`),
     no máx. `SLICE_DAYS` pedaços por execução;
   - grava checkpoint ao fim de cada pedaço OK (no `max_whenlogged` visto, ou no fim do
     pedaço se vazio); se `ok=False`, consolida e PARA (retoma na próxima execução);
   - ao alcançar `agora`, loga "Alcancado o presente" → regime realtime natural.
8. **Heartbeat** ao final (3.2).

O overlap (`OVERLAP_MINUTES`, default 10) relê a borda para não perder eventos; combinado
com `_id` idempotente, os re-vistos viram 409 benignos.

---

## 6. Estado atual dos pipelines (pipelines.yml)

Todos com `queue.type: persisted`. Workers = 2, exceto onde notado.

| pipeline.id | .conf | codec | worker=1? | Observações |
|-------------|-------|-------|-----------|-------------|
| cyberark-loginapp | 05-cyberark-loginapp-poller.conf | (poller http) | **sim** | Checkpoint anti-buraco; workers=1 obrigatório |
| sgp-recordings | sgp-recordings.conf | plain | não | Corrigido do crash json_lines |
| sws-recordings-realtime | sws-recordings-realtime.conf | json_lines | **sim** | reescrita (fonte Alero, `pipelines/sws-recordings/`); substituiu o `sws-recordings` antigo (script incompleto), **mesmo nome de data stream** (`logs-sws.recordings-default`); step-level, checkpoint+overlap; ver 3.12 |
| sws-recordings-reconcile | sws-recordings-reconcile.conf | json_lines | **sim** | par do acima; `schedule` diário, janela larga sem checkpoint, cobre sessão longa que fecha tarde (3.12) |
| sws-sessions-realtime | sws-sessions-realtime.conf | plain | não | Idem; `_id = _doc_id`; `interval=${POLL_INTERVAL_SECONDS:15}` |
| cyberark-audit-realtime | cyberark-audit-realtime.conf | — | não | Funciona; 409 dedup benigno |
| usersselo-snapshot | usersselo-snapshot.conf | json_lines | **sim** | Dump 2x/dia `schedule => "0 6,18 * * *"`; heartbeat |
| loginprodesp-realtime | loginprodesp-realtime.conf | json_lines | **sim** | Backfill+realtime fatiado; checkpoint; heartbeat; a saga inteira do §3 |
| (okta / azure_ad) | — | — | — | Linhas comentadas na unit — futuro |

### EnvironmentFiles (unit systemd `/usr/lib/systemd/system/logstash.service`)
Sem hífen (obrigatórios) — todos em `/etc/logstash/`, chmod 600:
`envio_cyberark_loginapp.env`, `envio_sws_recordings.env`, `envio_sgp_recordings.env`,
`envio_cyberark_realtime.env`, `envio_sws_sessions.env`, `envio_usersselo.env`,
`envio_loginprodesp.env`.

Cada `.env` traz tipicamente: `ELASTIC_URL`, `ELASTIC_USER`, `ELASTIC_PASSWORD`, e a var do
data stream do pipeline (ex.: `LOGINPRODESP_DATA_STREAM`). ⚠️ Nome da var de usuário difere
entre pipelines antigos e novos: `ELASTIC_USER` (novos, padrão) vs `ELASTIC_USERNAME`
(loginapp antigo) — confira qual o `.conf` referencia.

---

## 7. Data streams e ILM

Convenção de nome: `logs-cyberark.<fonte>-default` (ou `logs-sws.<fonte>-default`,
`logs-sgp.<fonte>-default`).

Índices em produção (exemplos):
- `logs-cyberark.loginprodesp-default` — login Prodesp; ILM **hot 30d → frozen**, sem delete.
  ~9.9M docs após backfill de 90 dias.
- `logs-cyberark.usersselo-default` — snapshot users/Selo 2x/dia; ILM **hot 7d → frozen**.
- `logs-sgp.recordings-default`, `logs-sws.recordings-default`, `logs-sws.sessions-default`,
  `logs-cyberark.loginapp-default`, mais o audit.

### bootstrap_elastic.sh (padrão)
Cria: ILM policy (hot rollover `max_age` + `max_primary_shard_size: 50gb`, `set_priority` →
frozen `searchable_snapshot` em `found-snapshots`, **sem delete**), index template de data
stream (com mappings + `data_stream: {}` + constant_keyword dataset), e o data stream.
Idempotente (reexecutar atualiza policy/template). Trata CRLF, verifica o repo de snapshot.
Lê `.env`/`env.env` com: `ELASTIC_URL`, `ELASTIC_USERNAME`, `ELASTIC_PASSWORD`,
`ELASTIC_INDEX`, `ELASTIC_TEMPLATE`, `ELASTIC_ILM_POLICY`, `ELASTIC_SNAPSHOT_REPOSITORY`,
e opcionalmente `KIBANA_URL` / `KIBANA_DATA_VIEW_NAME` para criar o data view (POST
`/api/data_views/data_view` com header `kbn-xsrf: true`).

**Rodar o bootstrap ANTES do pipeline produzir dados** (senão os docs falham na escrita —
com `create` não derruba o pipeline, mas não grava).

---

## 8. Credenciais das fontes CyberArk

⚠️ Todas em texto puro nos workers — candidatas a externalizar via `os.environ`.

| Fonte | tenant / token endpoint | client_id | client_secret |
|-------|--------------------------|-----------|----------------|
| users/Selo | `abb4724.id.cyberark.cloud/OAuth2/Token/PainelDetran` | `user_bi_elastic` | `UZ}RnDfi\|8#b]hvoh69uSh4[pa[NG/-9\|oKL>\XS` |
| login Prodesp | `prodesp.id.cyberark.cloud/OAuth2/Token/PainelProdesp` | `elastic_bi_asper` | `pya_.=ioq9i<}CgUJcb^4V~I}j8L/jxd9hsM]vW7` |

Query endpoint (Redrock): `https://<tenant>.id.cyberark.cloud/Redrock/query`, POST com
`{"Script": "...", "Args": {"PageNumber": N, "PageSize": M, "Caching": -1}}`.

### Peculiaridades por tenant (validar SEMPRE por tenant)
- **Prodesp**: `ApplicationType` é **NULL** em todos os logins (não filtrar por ele!).
  O `INNER JOIN User↔Event` **estoura timeout** — usar só a tabela `Event`
  (`E.NormalizedUser AS LoginName`). `Uuid` **não é único entre dias** (ver 3.8).
- Campos podem existir num tenant e não noutro; unicidade de identificadores não é
  garantida. **Sempre probe manual antes de assumir.**

### Probe manual (padrão de diagnóstico)
```bash
TOKEN=$(curl -sS -X POST "<URL_TOKEN>" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'client_id=<ID>' --data-urlencode 'client_secret=<SECRET>' \
  --data-urlencode 'grant_type=client_credentials' --data-urlencode 'scope=all' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

curl -sS --max-time 120 -X POST "<URL_QUERY>" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"Script":"SELECT ... WHERE ... AND E.WhenOccurred > Datefunc(\"2026-01-01 00:00:00\")","Args":{"PageNumber":1,"PageSize":5,"Caching":-1}}'
```
Sempre com **janela pequena** (senão estoura, ver 3.6). `--data-urlencode` protege os
caracteres especiais dos secrets. Datas do `Datefunc` entre aspas simples, formato
`YYYY-MM-DD HH:MM:SS`.

---

## 9. Monitoramento e saúde

### Script `/usr/local/bin/ls_health.py`
Lê o JSON de `_node/stats` via stdin e resume nó + pipelines. Uso:
```bash
curl -s --max-time 10 "http://127.0.0.1:9600/_node/stats/jvm,process,pipelines" \
  | python3 /usr/local/bin/ls_health.py
```
(Recriar com o conteúdo padrão se sumir — evita o inferno de escape do `python3 -c`.)

### Comandos úteis
```bash
# saúde do nó + pipelines (in/out/filtered, fila, reloads)
curl -s --max-time 10 "http://127.0.0.1:9600/_node/stats/jvm,process,pipelines" | python3 /usr/local/bin/ls_health.py

# stats de um pipeline
curl -s --max-time 10 "http://127.0.0.1:9600/_node/stats/pipelines/<id>" \
  | python3 -c 'import sys,json;d=json.load(sys.stdin)["pipelines"]["<id>"]["events"];print("in=",d["in"],"out=",d["out"],"filtered=",d["filtered"])'

# gargalo por plugin
curl -s --max-time 10 "http://127.0.0.1:9600/_node/stats/pipelines/<id>?vertices=true&pretty" | grep -A4 '"id"'

# count de um índice
curl -s -u '<user>:<pass>' "<ELASTIC_URL>/<index>/_count"

# distribuição por dia
curl -s -u '<user>:<pass>' "<ELASTIC_URL>/<index>/_search" -H 'Content-Type: application/json' \
  -d '{"size":0,"aggs":{"por_dia":{"date_histogram":{"field":"@timestamp","calendar_interval":"day"}}}}'

# @timestamp mais recente (o evento mais novo ingerido)
curl -s -u '<user>:<pass>' "<ELASTIC_URL>/<index>/_search" -H 'Content-Type: application/json' \
  -d '{"size":0,"aggs":{"ult":{"max":{"field":"@timestamp"}}}}'

# saúde do cluster
curl -s -u '<user>:<pass>' "<ELASTIC_URL>/_cluster/health?pretty"

# índices backing de um data stream (contagem real por índice)
curl -s -u '<user>:<pass>' "<ELASTIC_URL>/_cat/indices/.ds-<data-stream>-*?v&h=index,docs.count,store.size&s=index"
```

### Debug de worker isolado (sem envolver o Logstash)
```bash
# roda o worker na mão, checkpoint de teste em /tmp, conta linhas e vê o stderr
echo -n "2026-07-21T14:00:00+00:00" > /tmp/cp_test
sudo -u logstash PRODESP_SLICE_DAYS=1 PRODESP_CHECKPOINT_FILE=/tmp/cp_test \
  /usr/bin/python3 /etc/logstash/pipelines/<worker>.py 2>/tmp/dbg.log 1>/tmp/dbg.ndjson
wc -l < /tmp/dbg.ndjson      # linhas geradas (deve ter volume + heartbeat)
cat /tmp/dbg.log             # stderr completo (janela, recebidos, retry, erros)
```
**Sempre** validar sintaxe após copiar para produção (evita `IndentationError` que impede
o worker de rodar):
```bash
python3 -c "import ast; ast.parse(open('/etc/logstash/pipelines/<worker>.py').read()); print('OK')"
```

---

## 10. Procedimento para ADICIONAR um novo pipeline

1. **Entender a fonte** com probe manual (§8): endpoint, query, quais campos existem, qual é
   único de verdade, volume por dia, se ApplicationType/join servem.
2. **Decidir o codec** por volume (§3.1): pequeno→plain; grande/dump→json_lines+heartbeat.
3. **Escrever o worker** (§5): UTC em tudo (3.5), janela ≤1 dia (3.6), retry (3.7),
   `_id` único real (3.8), heartbeat se json_lines (3.2), NDJSON no stdout / logs no stderr.
4. **Escrever o `.conf`** (§4): sem `terminator` (3.4), `date` para @timestamp (3.9),
   output create com `_id`.
5. **`bootstrap_elastic.sh`** (§7): criar ILM + template + data stream ANTES de ligar.
   Definir fases hot/frozen conforme a retenção desejada; sem delete.
6. **`.env` do systemd** em `/etc/logstash/` (chmod 600) + linha `EnvironmentFile=` (sem
   hífen) na unit (3.10) + `systemctl daemon-reload`.
7. **Registrar no `pipelines.yml`** (workers=1 se o pipeline tem estado/checkpoint).
8. **Criar o diretório de checkpoint** `/var/lib/logstash/<pipeline>/` (dono logstash).
9. **Testar o worker isolado** (§9) ANTES de reiniciar. Validar sintaxe.
10. **Reiniciar** (2-3 min de boot) e validar com ls_health + count.
11. Backfill: apagar checkpoint dispara varredura do zero; **não reiniciar durante** (3.11).

---

## 11. Itens pendentes / dívidas conhecidas

- **Rotacionar credenciais** — Elastic e CyberArk estão em texto puro (workers + .env).
  Migrar para `os.environ` / secret manager.
- **Data view do Kibana** do loginprodesp — confirmar `KIBANA_URL` e criar (ficou em aberto).
- **Heap 20G** — uso real ~17%; poderia baixar para 8-10G e agilizar restarts. Não urgente.
- **Dimensionamento de shards** do loginprodesp — ~10M docs; revisar se o hot 30d comporta.
- **Snapshot usersselo** — validar 1º ciclo do cron 6h/18h no `logs-cyberark.usersselo-default`.
- Pipelines futuros previstos: **okta**, **azure_ad** (linhas já comentadas na unit).

---

## 12. Fontes já ingeridas (mapa rápido fonte → índice)

| Worker / fonte | Índice / data stream |
|----------------|----------------------|
| users/Selo (dump tabela User, parse Selo_) | `logs-cyberark.usersselo-default` |
| login Prodesp (Cloud.Core.Login, tabela Event) | `logs-cyberark.loginprodesp-default` |
| loginapp (poller CyberArk Detran) | `logs-cyberark.loginapp-default` |
| SGP recordings | `logs-sgp.recordings-default` |
| SWS recordings (reescrita, Alero, step-level; `pipelines/sws-recordings/`) | `logs-sws.recordings-default` |
| SWS sessions | `logs-sws.sessions-default` |
| cyberark audit | (data stream de audit) |

> Se precisar reconstruir um pipeline: identifique a fonte pelo endpoint/tabela no topo do
> worker, cruze com esta tabela para achar o índice, e siga o §10.
