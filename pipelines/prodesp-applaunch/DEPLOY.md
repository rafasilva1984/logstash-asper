# Deploy — pipeline `prodesp-applaunch-realtime` (novo, CyberArk tenant Prodesp)

Runbook de produção. Leia antes o `CLAUDE.md` da raiz (§3 e §10).

## O que é

Porta o script do cliente que exportava CSV (`usuarios_ultimas_12h_parte_N.csv`)
com os **acessos a aplicações** do tenant Prodesp: evento
`Cloud.Saas.Application.AppLaunch`, `ApplicationType` Web/OpenIDConnect/OAuth
(SOU SP, Plataforma SP, integrações OIDC...). Cada evento é enriquecido com os
dados do usuário (DisplayName, Status, UserType, ID, `Selo_`, LastLogin) e
com geolocalização do IP. O pipeline roda **a cada 5 min**, com janela de
~10 min, e gera um documento por evento.

| Item | Valor |
|------|-------|
| pipeline.id | `prodesp-applaunch-realtime` (workers=1, tem checkpoint) |
| Worker / conf | `prodesp_applaunch_worker.py` / `prodesp-applaunch-realtime.conf` |
| Codec | `json_lines` + heartbeat (o catch-up pode ser grande, CLAUDE.md 3.1/3.2) |
| Data stream | `logs-cyberark.prodespapplaunch-default` |
| ILM | `logs-cyberark.prodespapplaunch-ilm`: hot 30d (rollover 7d) → frozen, **sem delete** |
| Template | `logs-cyberark.prodespapplaunch-template` |
| `.env` | `/etc/logstash/envio_prodesp_applaunch.env` (prefixo `PRODESP_APPLAUNCH_`) |
| Checkpoint | `/var/lib/logstash/prodesp-applaunch/checkpoint` |

Não confundir com o `loginprodesp-realtime` (`logs-cyberark.loginprodesp-default`).
Ele usa o mesmo tenant e a mesma credencial, mas só o evento de **login**, sem os
dados do usuário. Este pipeline é **novo**, com data stream próprio, e o antigo
não é alterado.

**ILM, template e data stream já foram criados no Elastic em 2026-10-21**
(`bootstrap_prodesp_applaunch.sh` rodado da estação). Reexecutar é idempotente.
O data stream já tem ~700 docs de teste reais (AppLaunch de 2026-10-22 01:20Z a
01:40Z), com o mesmo `_id` que o pipeline vai gerar.

### Diferenças em relação ao script original (medidas por probe em 2026-10-21)

- **Sem o JOIN em SQL.** O `User INNER JOIN Event` do script estourou o timeout
  (180s) **mesmo numa janela de 10 min**, igual ao caso do loginprodesp
  (CLAUDE.md §8). O worker faz:
  1. eventos só na tabela `Event` (mesmos filtros), em <1s;
  2. prefetch `User.LastLogin > início − 120min`, em ~1s, que cobre ~95% dos logins;
  3. o resto via `User.LoginName IN (...)`, com custo **fixo de ~30s por query**,
     em lotes que cabem no **limite de 10.000 caracteres** da query;
  4. junta os dois no Python.

  Ciclo medido: ~35s.
- **Evento sem usuário na `User` é mantido**, com `user_found=false` e campos de
  usuário nulos. O INNER JOIN o descartava. Na amostra foram 0 casos.
- **`_id`**: o `Event.Uuid` vem **NULL** em todos os AppLaunch, então o `_id` é
  o sha256 de `WhenOccurred(ms)|LoginName|FromIP|ClientIP|ApplicationName|AppName|ApplicationType|EventType|Uuid`.
  Na amostra, 100% dos `_id` foram distintos.
- **Janela por checkpoint + overlap de 5 min**, e não "última 1h fixa". A cada
  5 min a janela é `(checkpoint − 5min) → agora`, ou seja, ~10 min, como na
  proposta. Se ficar parado, o pipeline recupera em fatias de 1h (até 6 por
  execução). O que for revisto vira 409, que é dedup benigno.
- **Campos novos derivados**: `Selo_level` (gold/silver/bronze),
  `Selo_reliabilities` (`[{id, updatedAt}]`), `Location` (geo_point
  lat/lon, para mapa no Grafana/Kibana), `user_found`. A coluna `User.ID` virou
  `UserID`. As datas (`WhenOccurred`, `WhenLogged`, `LastLogin`) são ISO UTC, e
  `@timestamp` = `WhenOccurred`.
- **Credenciais fora do código**, só no `.env`.

## Passo a passo (no `logstash-prod-01`)

### 1. Copiar arquivos

```bash
scp pipelines/prodesp-applaunch/prodesp_applaunch_worker.py pipelines/prodesp-applaunch/prodesp-applaunch-realtime.conf \
    root@logstash-prod-01:/etc/logstash/pipelines/
scp pipelines/prodesp-applaunch/bootstrap_prodesp_applaunch.sh pipelines/prodesp-applaunch/run_load_now.sh \
    pipelines/prodesp-applaunch/prodesp_applaunch_push.py \
    root@logstash-prod-01:/mnt/asper/scripts/
scp pipelines/prodesp-applaunch/envio_prodesp_applaunch.env \
    root@logstash-prod-01:/etc/logstash/envio_prodesp_applaunch.env   # o REAL (gitignored, ja preenchido na estacao)
```

```bash
python3 -c "import ast; ast.parse(open('/etc/logstash/pipelines/prodesp_applaunch_worker.py').read()); print('OK')"
chmod +x /mnt/asper/scripts/bootstrap_prodesp_applaunch.sh /mnt/asper/scripts/run_load_now.sh
```

O nome `run_load_now.sh` também existe no sgp/detran-steps. Se for manter
todos em `/mnt/asper/scripts/`, copie com sufixo: `run_load_now_applaunch.sh`.
Ele procura o `prodesp_applaunch_push.py` na mesma pasta.

### 2. `.env`

Se o `.env` real não foi copiado no passo 1: `cp envio_prodesp_applaunch.env.example`
e preencha os `<...>`.

```bash
chown logstash:logstash /etc/logstash/envio_prodesp_applaunch.env
chmod 600 /etc/logstash/envio_prodesp_applaunch.env
```

### 3. Diretório de checkpoint

```bash
mkdir -p /var/lib/logstash/prodesp-applaunch
chown logstash:logstash /var/lib/logstash/prodesp-applaunch
```

### 4. Bootstrap (ILM + template + data stream)

Já foi rodado. Reexecute só se mudar a policy ou o template:

```bash
set -a; source /etc/logstash/envio_prodesp_applaunch.env; set +a
/mnt/asper/scripts/bootstrap_prodesp_applaunch.sh
```

### 5. Teste do worker no servidor (antes do restart)

```bash
set -a; source /etc/logstash/envio_prodesp_applaunch.env; set +a
cd /mnt/asper/scripts
sudo -E -u logstash ./run_load_now.sh
```

O script roda em modo checkpoint de verdade (última 1h na 1ª vez), grava o
checkpoint, faz o push e mostra o total, os top apps, `Selo_level`, o min/max de
`@timestamp` e o checkpoint. Quando o pipeline for ligado, ele continua desse
checkpoint.

### 6. `pipelines.yml`

```yaml
- pipeline.id: prodesp-applaunch-realtime
  path.config: "/etc/logstash/pipelines/prodesp-applaunch-realtime.conf"
  queue.type: persisted
  pipeline.workers: 1   # worker tem checkpoint
```

### 7. Unit systemd

Em `/usr/lib/systemd/system/logstash.service`, **sem hífen** (CLAUDE.md 3.10).
O `.env` do passo 2 já precisa existir:

```
EnvironmentFile=/etc/logstash/envio_prodesp_applaunch.env
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

Depois de ~10 min:

```bash
cat /var/lib/logstash/prodesp-applaunch/checkpoint          # deve andar a cada 5 min
curl -s -u "$ELASTIC_USER:$ELASTIC_PASSWORD" "${ELASTIC_URL}/logs-cyberark.prodespapplaunch-default/_search" \
  -H 'Content-Type: application/json' -d '{"size":0,"aggs":{"ult":{"max":{"field":"@timestamp"}}}}'   # ~agora
grep -i prodesp_applaunch /var/log/logstash/logstash-plain.log | tail
```

409 no log é normal (overlap de 5 min).

## Backfill (opcional)

O cliente não pediu histórico. Para carregar, por exemplo, 30 dias: **antes da
1ª execução** (sem checkpoint), ponha `PRODESP_APPLAUNCH_INITIAL_LOOKBACK_HOURS=720`.
Outra opção é rodar `run_load_now.sh --since ... --until ...` em blocos, que não
mexe no checkpoint.

Custo estimado: para fatias fora do prefetch (>6h atrás), todo login sai pelo
`IN`, a ~30s por lote de ~600 logins. Uma hora de pico pode levar alguns minutos,
e 30 dias, várias horas. Com 6 fatias por execução, o realtime fica atrasado
durante o backfill e depois se alcança sozinho.

## Pontos para confirmar com o cliente

- **Evento sem usuário** é mantido (`user_found=false`), ao contrário do INNER
  JOIN original.
- **Overlap de 5 min** segue a proposta (janela ~10 min). Se aparecer evento
  atrasado na origem, aumente `PRODESP_APPLAUNCH_OVERLAP_MINUTES`. Na amostra,
  `WhenLogged == WhenOccurred` em 100% dos casos.
- **Volume medido**: ~380-580 eventos a cada 10-15 min à noite (~55k+/dia).
  O pico diurno não foi medido. O ciclo é dominado pelo `IN` (~30s por lote),
  com folga para os 5 min.
