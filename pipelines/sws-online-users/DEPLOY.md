# Deploy — pipeline `sws-online-users` (novo, fonte Alero)

Runbook para colocar em produção o pipeline novo, sem repetir as armadilhas
descritas no `CLAUDE.md` da raiz do repo (leia-o antes, principalmente §3 e
§10).

## O que é

Baseado no script de referência do cliente (`novopipeline/recordings_final_prod.py`,
`Volume_sessões.txt`): a cada execução, varre as sessions Alero das últimas
`SWS_ONLINE_LOOKBACK_HOURS` (default 13h, em chunks de 1h — a mesma janela
larga do script original) e, para cada uma, checa se teve algum step nos
últimos `SWS_ONLINE_ACTIVITY_MINUTES` (default 15min) via
`/sws/sessions/{id}/steps`. O lookback largo dentro de uma única execução é
o que evita perder sessão longa que começou muito antes da janela de
atividade (mesmo problema do CLAUDE.md 3.12) — por isso **não precisa** de
um segundo pipeline de reconciliação, diferente do `sws-recordings`.

Diferenças em relação ao script original:
- **Sem saída em XLSX** — o worker só emite NDJSON no stdout (arquitetura
  padrão da plataforma, ver CLAUDE.md §2); o relatório passa a ser feito no
  Kibana a partir do novo data stream.
- **Cache local vira estado persistente**: o `recordings_online_cache.json`
  (arquivo solto no diretório de execução) virou
  `SWS_ONLINE_STATE_FILE=/var/lib/logstash/sws-online-users/state.json`,
  gravação atômica (`.tmp` + rename), mesmo padrão do checkpoint dos outros
  workers.
- Cada usuário ativo agora vira um documento por execução, com `status`:
  - `ENTROU` — ficou ativo agora, não estava ativo na execução anterior;
  - `ATIVO` — continua ativo desde a execução anterior;
  - `SAIU` — estava ativo na execução anterior, não está mais.
  Isso reproduz exatamente os três blocos que o script original imprimia
  (ENTROU / SAIU / COM ATIVIDADE AGORA), só que como série temporal no
  Elastic em vez de print + Excel.
- **Sem heartbeat**: usa `codec => plain` (saída pequena, dezenas de
  usuários por execução) — heartbeat só é necessário com `json_lines`
  (CLAUDE.md 3.1/3.2).

## Passo a passo (no servidor `logstash-prod-01`)

### 1. Copiar os arquivos para o servidor

```bash
scp pipelines/sws-online-users/sws_online_users_worker.py    root@logstash-prod-01:/etc/logstash/pipelines/
scp pipelines/sws-online-users/sws-online-users.conf         root@logstash-prod-01:/etc/logstash/pipelines/
scp pipelines/sws-online-users/bootstrap_sws_online_users.sh root@logstash-prod-01:/mnt/asper/scripts/
```

> `.conf` e worker `.py` sempre em `/etc/logstash/pipelines/`; scripts
> auxiliares (`bootstrap_*.sh`) em `/mnt/asper/scripts/` (CLAUDE.md §1).

Valide a sintaxe do worker antes de seguir:

```bash
python3 -c "import ast; ast.parse(open('/etc/logstash/pipelines/sws_online_users_worker.py').read()); print('OK')"
```

### 2. Criar o `.env`

```bash
cp envio_sws_online_users.env.example /etc/logstash/envio_sws_online_users.env
chown logstash:logstash /etc/logstash/envio_sws_online_users.env
chmod 600 /etc/logstash/envio_sws_online_users.env
```

Credenciais Alero são as mesmas do `sws-recordings` (mesmo tenant/service
account) — só confirme se ainda são válidas antes de subir.

### 3. Criar o diretório de estado

```bash
mkdir -p /var/lib/logstash/sws-online-users
chown logstash:logstash /var/lib/logstash/sws-online-users
```

### 4. Bootstrap do template/ILM/data stream

```bash
set -a; source /etc/logstash/envio_sws_online_users.env; set +a
export ELASTIC_USERNAME="$ELASTIC_USER"   # o bootstrap espera ELASTIC_USERNAME

cd /mnt/asper/scripts
./bootstrap_sws_online_users.sh
```

Cria: ILM policy (hot `${SWS_ONLINE_ILM_HOT_MAX_AGE:-30d}` → frozen em
`found-snapshots`, sem delete — volume baixo, retenção maior por padrão),
index template com o mapping, e o data stream
`logs-sws.online-users-default`. Se `KIBANA_URL` estiver setado no `.env`,
cria o data view também.

### 5. Testar o worker isolado (sem envolver o Logstash)

```bash
set -a; source /etc/logstash/envio_sws_online_users.env; set +a
SWS_ONLINE_STATE_FILE=/tmp/sws_online_state_test.json \
  sudo -E -u logstash /usr/bin/python3 /etc/logstash/pipelines/sws_online_users_worker.py \
  2>/tmp/dbg.log 1>/tmp/dbg.ndjson

wc -l < /tmp/dbg.ndjson     # numero de usuarios (entrou+ativo+saiu) nesta execucao
cat /tmp/dbg.log            # janela, sessions varridas, ativos, retries, erros
```

Rode uma segunda vez (mesmo `SWS_ONLINE_STATE_FILE`) para ver o diff
ENTROU/SAIU funcionando de fato entre duas execuções.

### 6. Registrar no `pipelines.yml`

```yaml
- pipeline.id: sws-online-users
  path.config: "/etc/logstash/pipelines/sws-online-users.conf"
  queue.type: persisted
  pipeline.workers: 1   # worker tem estado em disco — evita corrida entre workers
```

### 7. Adicionar o `EnvironmentFile` na unit systemd

Editar `/usr/lib/systemd/system/logstash.service` e adicionar (sem hífen —
CLAUDE.md 3.10, arquivo já precisa existir do passo 2 antes de reiniciar):

```
EnvironmentFile=/etc/logstash/envio_sws_online_users.env
```

```bash
systemctl daemon-reload
```

### 8. Reiniciar e validar

```bash
systemctl restart logstash   # 2-3 min de boot, API 9600 nao responde nesse tempo
```

```bash
curl -s --max-time 10 "http://127.0.0.1:9600/_node/stats/jvm,process,pipelines" | python3 /usr/local/bin/ls_health.py

curl -s -u "$ELASTIC_USER:$ELASTIC_PASSWORD" "${ELASTIC_URL}/logs-sws.online-users-default/_count"

curl -s -u "$ELASTIC_USER:$ELASTIC_PASSWORD" "${ELASTIC_URL}/logs-sws.online-users-default/_search" \
  -H 'Content-Type: application/json' \
  -d '{"size":0,"aggs":{"por_status":{"terms":{"field":"status"}}}}'
```

Espera-se, depois de ~15-30min: contagem de docs crescendo a cada execução
(`SWS_ONLINE_INTERVAL_SECONDS`, default 900s) e a agregação `por_status`
mostrando `ATIVO`/`ENTROU`/`SAIU`.

## Handoff ao cliente

- Novo data stream `logs-sws.online-users-default`: snapshot de usuários
  online a cada 15min (configurável), com `status` (`ENTROU`/`ATIVO`/`SAIU`),
  aplicações em uso, quantidade de sessões e horário de início da sessão
  mais antiga ainda ativa.
- Substitui o relatório manual em XLSX do script original — o mesmo dado
  (e histórico, já que fica em série temporal) fica disponível via
  Discover/dashboard no Kibana.
- Criar/atualizar o **data view** no Kibana apontando para
  `logs-sws.online-users-default` (o bootstrap já cria automaticamente se
  `KIBANA_URL` estiver setado no `.env`).
- `SWS_ONLINE_MAX_WORKERS` default aqui é 50 (script original usava 100) —
  ajustar no `.env` se o tempo de execução (`in`/`out` do pipeline, ver
  `ls_health.py`) ficar alto; o host tem 32 vCPU de folga.
- Pendência conhecida (igual ao resto da plataforma): credenciais Alero e
  Elastic em texto puro no `.env` — rotacionar quando o projeto for para
  produção definitiva.
- ILM default: hot 30d → frozen, sem delete. Confirmar com o cliente se a
  retenção exige outro valor (`SWS_ONLINE_ILM_HOT_MAX_AGE`).
