# Deploy — substituição do pipeline `sws-recordings` (reescrita, fonte Alero)

Runbook para trocar o pipeline antigo (script incompleto) por este, sem
repetir as armadilhas descritas no `CLAUDE.md` da raiz do repo (leia-o
antes, principalmente §3 e §10).

## O que mudou

- Fonte antiga → nova: worker reescrito com base no script de exportação
  Excel/CSV da API **Alero** (`recordings_unificadas.py`), removendo:
  - o agrupamento por minuto (`aggregate_by_minute`) — não existe mais;
    cada `sessionStep` vira **um documento**, em tempo real;
  - a saída em Excel/CSV e os relatórios de auditoria em planilha;
  - os modos de linha de comando (`d1`/`min`) — substituídos por um
    **checkpoint persistente** unificado (mesmo padrão do `loginprodesp`).
- Passa a ser **dois pipelines Logstash**, mesmo worker (`--mode`):
  - `sws-recordings-realtime` — roda a cada
    `SWS_REALTIME_INTERVAL_SECONDS` (default 60s), com checkpoint+overlap.
  - `sws-recordings-reconcile` — roda 1x/dia (`schedule`, default 4h da
    manhã), varrendo as últimas `SWS_RECONCILE_LOOKBACK_HOURS` (default
    48h) sem depender de checkpoint.
  - **Por quê dois:** a API da Alero filtra `recordings` pelo horário de
    **início** da sessão e só devolve sessão **fechada**. Uma sessão longa
    que começou fora da janela realtime pode nunca ser vista por ela. O
    reconcile resolve isso revarrendo um período largo todo dia — como o
    `_id` é determinístico e o output usa `action=create`, tudo que já foi
    indexado vira `409` (dedup benigno); só o que faltou entra de fato.
    Isso é o equivalente do modo `D1` do script antigo, só que automático.
- Índice: mesmo nome, `logs-sws.recordings-default`, mas **schema novo**
  (step-level, com todos os campos de `sessionSteps` — clique, navegação,
  troca de valor, colagem via clipboard etc., não só o resumo da sessão).

## Passo a passo (no servidor `logstash-prod-01`)

### 0. Confirmar com o cliente

O índice atual será **apagado e recriado com o mesmo nome** — os dados
antigos (schema incompleto) se perdem. Confirme isso antes do passo 4.

### 1. Copiar os arquivos para o servidor

```bash
scp pipelines/sws-recordings/sws_recordings_worker.py       root@logstash-prod-01:/etc/logstash/pipelines/
scp pipelines/sws-recordings/sws-recordings-realtime.conf   root@logstash-prod-01:/etc/logstash/pipelines/
scp pipelines/sws-recordings/sws-recordings-reconcile.conf  root@logstash-prod-01:/etc/logstash/pipelines/
scp pipelines/sws-recordings/bootstrap_sws_recordings.sh    root@logstash-prod-01:/mnt/asper/scripts/
```

> `.conf` e workers `.py` sempre em `/etc/logstash/pipelines/`; scripts auxiliares
> (`bootstrap_*.sh`) em `/mnt/asper/scripts/` (ver CLAUDE.md §1).

Valide a sintaxe do worker antes de seguir:

```bash
python3 -c "import ast; ast.parse(open('/etc/logstash/pipelines/sws_recordings_worker.py').read()); print('OK')"
```

### 2. Parar o pipeline antigo

Comente/remova a entrada `sws-recordings` (a antiga) do `pipelines.yml`
**antes** de reiniciar (não misture schema antigo com o novo no mesmo
índice). Não reinicie ainda — vamos trocar tudo de uma vez no passo 7.

### 3. Atualizar o `.env`

O pipeline antigo já referenciava `envio_sws_recordings.env`. Reescreva o
conteúdo com base em `envio_sws_recordings.env.example` (mantém o mesmo
nome/caminho de arquivo, então **não precisa mexer** na linha
`EnvironmentFile=` da unit nem rodar `daemon-reload` por causa disso):

```bash
cp envio_sws_recordings.env.example /etc/logstash/envio_sws_recordings.env
chown logstash:logstash /etc/logstash/envio_sws_recordings.env
chmod 600 /etc/logstash/envio_sws_recordings.env
```

### 4. Criar o diretório de checkpoint

```bash
mkdir -p /var/lib/logstash/sws-recordings
chown logstash:logstash /var/lib/logstash/sws-recordings
```

### 5. Bootstrap do template/ILM + apagar o índice antigo

```bash
set -a; source /etc/logstash/envio_sws_recordings.env; set +a
export ELASTIC_USERNAME="$ELASTIC_USER"   # o bootstrap espera ELASTIC_USERNAME

cd /mnt/asper/scripts
./bootstrap_sws_recordings.sh --drop-existing
```

O `DELETE /_data_stream/logs-sws.recordings-default` apaga o data stream
**e todas as suas backing indices** (`.ds-logs-sws.recordings-default-*`)
de uma vez — não é preciso apagar cada `.ds-*` manualmente. Em seguida o
script recria, com **exatamente o mesmo nome** (`logs-sws.recordings-default`,
sem sufixo de versão): ILM policy (hot `${SWS_ILM_HOT_MAX_AGE:-15d}` →
frozen em `found-snapshots`, sem delete), index template com o mapping
novo, e o data stream. **Ajuste `SWS_ILM_HOT_MAX_AGE`/`SWS_ILM_HOT_MAX_SHARD_SIZE`**
no `.env` se o volume step-level exigir uma janela hot menor que os 15d
default (volume por sessão pode ser bem maior que o índice de sessão
"resumo" que existia antes).

### 6. Testar o worker isolado (sem envolver o Logstash)

```bash
set -a; source /etc/logstash/envio_sws_recordings.env; set +a
SWS_CHECKPOINT_FILE=/tmp/cp_sws_test SWS_BACKFILL_MINUTES=15 \
  sudo -E -u logstash /usr/bin/python3 /etc/logstash/pipelines/sws_recordings_worker.py --mode realtime \
  2>/tmp/dbg.log 1>/tmp/dbg.ndjson

wc -l < /tmp/dbg.ndjson     # deve ter volume + a linha de heartbeat
cat /tmp/dbg.log            # janela, recordings coletadas, retries, erros
```

Repita para `--mode reconcile` se quiser validar a janela larga também
(pode demorar mais, é 48h de histórico).

### 7. Registrar no `pipelines.yml`

```yaml
- pipeline.id: sws-recordings-realtime
  path.config: "/etc/logstash/pipelines/sws-recordings-realtime.conf"
  queue.type: persisted
  pipeline.workers: 1   # worker tem checkpoint em disco — evita corrida entre workers

- pipeline.id: sws-recordings-reconcile
  path.config: "/etc/logstash/pipelines/sws-recordings-reconcile.conf"
  queue.type: persisted
  pipeline.workers: 1
```

Remova a entrada antiga `sws-recordings` se ainda não tiver removido.

### 8. Reiniciar e validar

```bash
systemctl daemon-reload
systemctl restart logstash   # 2-3 min de boot, API 9600 nao responde nesse tempo
```

```bash
curl -s --max-time 10 "http://127.0.0.1:9600/_node/stats/jvm,process,pipelines" | python3 /usr/local/bin/ls_health.py

curl -s -u "$ELASTIC_USER:$ELASTIC_PASSWORD" "${ELASTIC_URL}/logs-sws.recordings-default/_count"

curl -s -u "$ELASTIC_USER:$ELASTIC_PASSWORD" "${ELASTIC_URL}/logs-sws.recordings-default/_search" \
  -H 'Content-Type: application/json' \
  -d '{"size":0,"aggs":{"ult":{"max":{"field":"@timestamp"}}}}'
```

### 9. Validar Alero × Elastic (sanity-check de volume)

`validate_sws_recordings.sh` consulta a Alero e o Elastic na mesma janela
de tempo (default: últimos 5 min) e imprime lado a lado a contagem de
recordings e de documentos de cada lado — útil para checar rapidamente
se a ingestão está acompanhando a origem:

```bash
set -a; source /etc/logstash/envio_sws_recordings.env; set +a
cd /mnt/asper/scripts
./validate_sws_recordings.sh          # ultimos 5 min (default)
./validate_sws_recordings.sh 30       # ou outra janela, em minutos
```

Ver o cabeçalho do script e o item 3.12 do `CLAUDE.md`: como a Alero
filtra pelo início da sessão (só fechada) e o Elastic indexa por horário
do evento (step), uma diferença pequena numa janela curta é esperada —
o reconcile diário cobre o que escapar dela. Diferença grande ou
persistente é que é sinal de problema real.

## Handoff ao cliente

- Índice `logs-sws.recordings-default` recriado com schema novo,
  step-level (clique, navegação, valor digitado/alterado, colagem via
  clipboard, com `extraction_status` por sessão para auditoria de
  completude).
- Ingestão em dois pipelines: `sws-recordings-realtime` (quase em tempo
  real) e `sws-recordings-reconcile` (varredura diária de reconciliação
  para sessões longas — explicar o porquê, ver seção acima).
- Criar/atualizar o **data view** no Kibana apontando para
  `logs-sws.recordings-default` (o bootstrap já cria automaticamente se
  `KIBANA_URL` estiver setado no `.env`).
- Pendência conhecida (igual ao resto da plataforma): credenciais da
  Alero e do Elastic em texto puro no `.env` — rotacionar quando o
  projeto for para produção definitiva.
- ILM default: hot 15d → frozen, sem delete. Confirmar com o cliente se
  a retenção/compliance exige outro valor (`SWS_ILM_HOT_MAX_AGE`).
