# Deploy — pipeline `detran-steps-daily` (novo, fonte Alero / tenant Detran)

Runbook de produção. Leia antes o `CLAUDE.md` da raiz (§3 e §10).

## O que é

Porta o script diário do Detran (`detran_recordings_diario.py`, que gerava
`saida_detran/steps_unificados.xlsx`) para a plataforma. É o mesmo desenho do
`sgp-steps-daily`, aplicado a outro tenant. Todo dia às **07:00**
(hora local do servidor, `America/Sao_Paulo`), o worker lista as sessões
Alero do tenant **sem filtro de busca** (`freeSearch` opcional, igual ao
original) **abertas nas últimas 24h** e emite **um
documento por sessionStep** no data stream `logs-detran.steps-default`.

| Item | Valor |
|------|-------|
| pipeline.id | `detran-steps-daily` (workers=1, tem checkpoint) |
| Worker / conf | `detran_steps_worker.py` / `detran-steps-daily.conf` |
| Codec | `json_lines` + heartbeat (volume grande, CLAUDE.md 3.1/3.2) |
| Data stream | `logs-detran.steps-default` |
| ILM | `logs-detran.steps-ilm`: hot 30d → frozen 300d → **delete** (330d) |
| `.env` | `/etc/logstash/envio_detran_steps.env` |
| Checkpoint | `/var/lib/logstash/detran-steps/checkpoint` |

**Mesmo tenant e mesma credencial do `sws-recordings`**
(`logs-sws.recordings-default`, realtime + reconcile, também step-level).
Os dados vão ser em boa parte os mesmos, em outro data stream. É intencional:
o pipeline é novo e isolado, e nada do `sws-recordings` é tocado. Também não
confundir com o `cyberark-detran-directory` (CyberArk Identity, prefixo
`DETRAN_*`), que é outra fonte.

### Diferenças em relação ao script original

- **Sem Excel**: o worker emite NDJSON e o relatório passa a ser feito no
  Kibana. Os campos são as mesmas colunas do xlsx, menos os `*_fmt` (as
  datas são `date` de verdade, e o Kibana exibe no fuso do navegador).
- **`_id` = `event_uid` do original** (sha256 de recording + step id). Com
  `action => create`, um reprocessamento vira 409, que é dedup benigno.
- **Checkpoint + overlap de 60min**: em operação normal, a janela é
  `(última execução − 60min) → agora`, ou seja, 24h + 1h. Se uma
  execução falhar ou não disparar, a seguinte cobre o buraco (até 7 dias,
  em fatias de 24h). O overlap revarre sessões que ainda estavam
  **abertas** no disparo anterior: a Alero só devolve sessão fechada e
  filtra pelo início (CLAUDE.md 3.12).
- **Sessão sem step** vira 1 doc com `extraction_status=SEM_STEPS`.
- **Falha de extração** numa sessão (depois dos retries): nada é gravado
  para ela, o checkpoint não avança e a próxima execução revarre a janela.
  O original abortava com exit 1.
- **Credenciais** fora do código, só no `.env`, com prefixo `DETRAN_STEPS_`.
  Todos os `.env` entram no mesmo processo do Logstash, e
  `ALERO_CLIENT_ID` já pertence ao `sws-recordings` com outra credencial.

## Passo a passo (no `logstash-prod-01`)

### 1. Copiar arquivos

```bash
scp pipelines/detran-steps/detran_steps_worker.py pipelines/detran-steps/detran-steps-daily.conf \
    root@logstash-prod-01:/etc/logstash/pipelines/
scp pipelines/detran-steps/bootstrap_detran_steps.sh pipelines/detran-steps/run_load_now.sh \
    pipelines/detran-steps/detran_steps_push.py \
    root@logstash-prod-01:/mnt/asper/scripts/
```

```bash
python3 -c "import ast; ast.parse(open('/etc/logstash/pipelines/detran_steps_worker.py').read()); print('OK')"
chmod +x /mnt/asper/scripts/bootstrap_detran_steps.sh /mnt/asper/scripts/run_load_now.sh
```

### 2. `.env`

```bash
cp envio_detran_steps.env.example /etc/logstash/envio_detran_steps.env
vi /etc/logstash/envio_detran_steps.env   # preencher ELASTIC_USER/PASSWORD e DETRAN_STEPS_CLIENT_ID/SECRET
chown logstash:logstash /etc/logstash/envio_detran_steps.env
chmod 600 /etc/logstash/envio_detran_steps.env
```

O `DETRAN_STEPS_CLIENT_ID`/`SECRET` são os do script original (o service
account Detran, hoje o **mesmo** valor do `ALERO_CLIENT_ID` do
`envio_sws_recordings.env`). Use a string inteira, com o `.ExternalServiceAccount`.

### 3. Diretório de checkpoint

```bash
mkdir -p /var/lib/logstash/detran-steps
chown logstash:logstash /var/lib/logstash/detran-steps
```

### 4. Bootstrap (ILM + template + data stream) — ANTES de qualquer carga

```bash
set -a; source /etc/logstash/envio_detran_steps.env; set +a
/mnt/asper/scripts/bootstrap_detran_steps.sh
```

O script aborta em qualquer HTTP diferente de 200. No final, ele mostra o
data stream com `ilm_policy= logs-detran.steps-ilm`.

### 5. Carga imediata (sem esperar as 07:00, sem reiniciar o Logstash)

```bash
set -a; source /etc/logstash/envio_detran_steps.env; set +a
cd /mnt/asper/scripts
sudo -E -u logstash ./run_load_now.sh
```

O script roda o worker em modo **agendado de verdade** (últimas 24h, grava
o checkpoint), salva o NDJSON em `/tmp`, faz o push via `_bulk` e mostra a
contagem, os docs por `extraction_status`, o min/max de `@timestamp` e o
checkpoint. A execução das 07:00 seguinte continua desse checkpoint.

- Se o push falhar depois que o worker gravou, reenvie o mesmo arquivo:
  `python3 detran_steps_push.py < /tmp/detran_steps_load_<stamp>.ndjson`.
  É idempotente.
- Para carregar um período específico **sem mexer no checkpoint**:
  `sudo -E -u logstash ./run_load_now.sh --since 2026-10-01T00:00:00-03:00 --until 2026-10-02T00:00:00-03:00`
  (as fatias são de no máximo 24h).

### 6. `pipelines.yml`

```yaml
- pipeline.id: detran-steps-daily
  path.config: "/etc/logstash/pipelines/detran-steps-daily.conf"
  queue.type: persisted
  pipeline.workers: 1   # worker tem checkpoint
```

### 7. Unit systemd

Em `/usr/lib/systemd/system/logstash.service`, adicionar **sem hífen**
(CLAUDE.md 3.10). O `.env` do passo 2 já precisa existir:

```
EnvironmentFile=/etc/logstash/envio_detran_steps.env
```

```bash
systemctl daemon-reload
```

### 8. Reiniciar e validar

Não reinicie com um backfill rodando em outro pipeline (3.11).

```bash
systemctl restart logstash   # 2-3 min de boot
curl -s --max-time 10 "http://127.0.0.1:9600/_node/stats/jvm,process,pipelines" | python3 /usr/local/bin/ls_health.py
```

O `detran-steps-daily` deve aparecer com `in=0` até as 07:00. Depois do
primeiro disparo:

```bash
cat /var/lib/logstash/detran-steps/checkpoint     # deve ser ~07:00 de hoje (em UTC = 10:00Z)
curl -s -u "$ELASTIC_USER:$ELASTIC_PASSWORD" "${ELASTIC_URL}/logs-detran.steps-default/_count"
grep -i detran_steps /var/log/logstash/logstash-plain.log | tail
```

## Pontos para confirmar com o cliente

- **Delete após 330 dias.** É a primeira policy da plataforma com fase de
  delete. Para manter os dados para sempre: `DETRAN_STEPS_ILM_DELETE=false` e
  reexecutar o bootstrap.
- **Janela 24h + 1h de overlap**, e não exatamente 24h. É o que evita
  perder sessão aberta no momento do disparo. Ajuste com
  `DETRAN_STEPS_OVERLAP_MINUTES`. Sessões que duram mais que o overlap
  continuam podendo escapar (limitação da API, 3.12). Se isso importar,
  aumente o overlap; o custo é só revarrer e gerar 409.
- **TLS**: o default é não validar certificado, igual ao script original
  (`DETRAN_STEPS_VERIFY_TLS`).
