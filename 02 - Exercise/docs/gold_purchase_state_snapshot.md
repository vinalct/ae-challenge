# Snapshot de Estado da Compra na Gold

## Objetivo

Descrever como o snapshot gold reconstrui o ultimo estado conhecido de cada compra para cada `snapshot_date`, sem vazar eventos futuros para snapshots antigos.

## Artefatos implementados

- DDL: [`sql/ddl/gold_purchase_state_snapshot.sql`](../sql/ddl/gold_purchase_state_snapshot.sql)
- Contratos gold: [`src/gold/contracts.py`](../src/gold/contracts.py)
- Logica de snapshot: [`src/gold/purchase_snapshot.py`](../src/gold/purchase_snapshot.py)
- Loader gold: [`src/gold/loader.py`](../src/gold/loader.py)
- Execucao local: [`scripts/build_gold_purchase_snapshot_local.py`](../scripts/build_gold_purchase_snapshot_local.py)

## Grao da tabela

- uma linha por `snapshot_date`, `purchase_id` e `purchase_partition`

## Logica de reconstrucao historica

1. Ler as quatro tabelas silver CDC.
2. Manter apenas linhas aptas para ordenacao as-of, com chave de negocio completa e colunas de ordenacao validas.
3. Construir um calendario diario entre a menor e a maior `transaction_date` observadas nas fontes silver.
4. Expandir cada compra para todos os `snapshot_date` a partir da sua primeira aparicao em `purchase`.
5. Para cada fonte, selecionar a versao mais recente disponivel quando `transaction_date <= snapshot_date`.
6. Juntar os ultimos estados de `purchase`, `purchase_extra_info`, `order_transaction_cost_hist` e `product_item`.

Isso garante carry-forward: quando uma fonte nao muda em um dia, o snapshot continua usando o ultimo estado conhecido daquela fonte.

## Regras de ordenacao "latest as of snapshot"

Quando mais de uma versao da mesma entidade esta disponivel ate a data de referencia, o desempate segue a ordem documentada no projeto:

1. `transaction_datetime` desc
2. `transaction_date` desc
3. `ingestion_ts` desc
4. `record_hash` desc

## Flags e qualidade

O snapshot publica:

- flags herdadas da silver, prefixadas por origem, como `purchase:missing_release_date`
- flags derivadas do proprio estado montado, como `missing_product_item_snapshot`
- `quality_status` resumido como `valid`, `warning` ou `error`

Compras incompletas continuam visiveis no snapshot, mas ficam com `is_metric_eligible = false`.

## Regras de elegibilidade de GMV no snapshot

Uma linha fica com `is_metric_eligible = true` somente quando:

- existe estado de `purchase`
- existe estado de `product_item`
- existe estado de `purchase_extra_info`
- `subsidiary` nao e nula
- `purchase_status = 'APROVADA'`
- `release_date` nao e nula
- `purchase_total_value` nao e nulo
- `purchase_total_value >= 0`

## Linhagem publicada

Para cada origem contribuinte, a tabela persiste:

- `*_source_transaction_datetime`
- `*_source_transaction_date`
- `*_source_record_hash`

## Como executar localmente

1. Carregar bronze e silver com `make build-silver`
2. Materializar o snapshot com `make build-gold-purchase-snapshot`
3. Inspecionar a tabela com `make pyspark`

Exemplo de consulta:

```sql
SELECT
    snapshot_date,
    purchase_id,
    subsidiary,
    is_metric_eligible,
    quality_status,
    quality_flags
FROM ae_challenge.gold_purchase_state_snapshot
ORDER BY snapshot_date, purchase_id;
```
