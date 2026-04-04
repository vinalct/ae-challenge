# GMV Diario por Subsidiaria na Gold

## Objetivo

Descrever como a agregacao final transforma `gold_purchase_state_snapshot` em uma tabela historica de GMV diario por subsidiaria, alem de expor um acesso simples para o snapshot mais recente.

## Artefatos implementados

- DDL da tabela final: [`sql/ddl/gold_gmv_daily_by_subsidiary_snapshot.sql`](../sql/ddl/gold_gmv_daily_by_subsidiary_snapshot.sql)
- DDL da view de acesso atual: [`sql/access/vw_gmv_daily_by_subsidiary_current.sql`](../sql/access/vw_gmv_daily_by_subsidiary_current.sql)
- Query final para consumo atual: [`sql/access/select_current_gmv_daily_by_subsidiary.sql`](../sql/access/select_current_gmv_daily_by_subsidiary.sql)
- Exemplo populado da saida final: [`examples/gold_gmv_daily_by_subsidiary_snapshot_example.csv`](../examples/gold_gmv_daily_by_subsidiary_snapshot_example.csv)
  O exemplo foi recortado para evidenciar entrada tardia no GMV, mudanca posterior de subsidiaria, snapshot de fim de marco e efeito de reembolso.
- Contratos gold: [`src/gold/contracts.py`](../src/gold/contracts.py)
- Agregacao PySpark: [`src/gold/gmv_daily.py`](../src/gold/gmv_daily.py)
- Loader e acesso atual: [`src/gold/gmv_loader.py`](../src/gold/gmv_loader.py)
- Execucao local: [`scripts/build_gold_gmv_daily_by_subsidiary_local.py`](../scripts/build_gold_gmv_daily_by_subsidiary_local.py)

## Grao da tabela final

- uma linha por `snapshot_date`, `gmv_date` e `subsidiary`

## Regras de negocio aplicadas

- usar somente linhas com `is_metric_eligible = true` vindas de `gold_purchase_state_snapshot`
- `gmv_date = release_date`, ja propagada no snapshot gold
- `gmv_daily_amount = sum(purchase_total_value)`
- `gmv_daily_purchase_count = quantidade distinta de compras elegiveis`
- `gmv_daily_item_quantity = soma de item_quantity`
- `gmv_mtd_amount` = acumulado mensal por `snapshot_date`, `subsidiary` e mes de `gmv_date`

## Qualidade agregada

O agregado final resume `quality_status` assim:

- `error` se alguma linha contribuinte estiver em `error`
- `warning` se nao houver `error`, mas existir pelo menos uma linha contribuinte em `warning`
- `valid` quando todas as linhas contribuintes estiverem em `valid`

## Acesso ao snapshot atual

Quando o catalogo oferece suporte a views persistidas, o loader recria `vw_gmv_daily_by_subsidiary_current` a cada carga e ela sempre aponta para o maior `snapshot_date` disponivel na tabela historica final.

No catalogo local do exercicio, views persistidas nao sao suportadas. Nessa execucao, o acesso atual deve ser feito diretamente a partir da tabela historica usando `MAX(snapshot_date)`.

Isso preserva dois padroes de acesso:

- historico: consultar `gold_gmv_daily_by_subsidiary_snapshot`
- atual: usar a view quando o catalogo suportar ou fazer uma consulta direta na tabela historica

## Como executar localmente

1. Materializar o snapshot gold com `make build-gold-purchase-snapshot`
2. Materializar o agregado final com `make build-gold-gmv-daily-by-subsidiary`
3. Inspecionar a tabela e, se suportado pelo catalogo, a view com `make pyspark`

Exemplos:

```sql
SELECT *
FROM ae_challenge.gold_gmv_daily_by_subsidiary_snapshot
ORDER BY snapshot_date, gmv_date, subsidiary;

WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM ae_challenge.gold_gmv_daily_by_subsidiary_snapshot
)
SELECT *
FROM ae_challenge.gold_gmv_daily_by_subsidiary_snapshot
WHERE snapshot_date = (SELECT snapshot_date FROM latest_snapshot)
ORDER BY gmv_date, subsidiary;
```
