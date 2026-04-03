# Padronizacao CDC da Camada Silver

## Objetivo

Descrever como a camada silver transforma os eventos raw da bronze em tabelas CDC tipadas, deduplicadas e prontas para a reconstrucao deterministica de estado.

## Artefatos implementados

- DDL: [`sql/ddl/silver_tables.sql`](../sql/ddl/silver_tables.sql)
- Contratos silver: [`src/silver/contracts.py`](../src/silver/contracts.py)
- Transformacoes CDC: [`src/silver/standardization.py`](../src/silver/standardization.py)
- Loader da silver: [`src/silver/loader.py`](../src/silver/loader.py)
- Execucao local: [`scripts/build_silver_local.py`](../scripts/build_silver_local.py)

## Tabelas silver

- `silver_purchase_cdc`
- `silver_product_item_cdc`
- `silver_purchase_extra_info_cdc`
- `silver_order_transaction_cost_hist_cdc`

## Regras de padronizacao aplicadas

- Reutilizar o contrato canonico tipado ja validado na bronze.
- Normalizar `purchase_status` com `trim + upper`.
- Normalizar `subsidiary` com `trim + lower`.
- Preservar os metadados bronze `ingestion_ts`, `batch_id`, `source_file` e `record_hash`.
- Publicar flags de qualidade em vez de descartar silenciosamente linhas problematicas.

## Logica de deduplicacao

- A deduplicacao remove apenas reenvios exatamente identicos.
- O agrupamento de duplicidade usa `business_key + record_hash`.
- Como `record_hash` e calculado apenas sobre o payload canonico, dois eventos com o mesmo hash representam o mesmo conteudo de negocio, ainda que tenham chegado em lotes diferentes.
- Quando existem copias identicas, a silver preserva a primeira observacao por `ingestion_ts asc`, `batch_id asc`, `source_file asc`.

Essa escolha evita que um resend tardio altere artificialmente a prioridade de ordenacao de uma versao que ja era conhecida.

## Logica de ordenacao de eventos

Cada tabela silver recebe colunas tecnicas para suportar a selecao "latest as of snapshot":

- `event_version_number`: ordem crescente da versao dentro da chave de negocio.
- `event_latest_rank`: ordem decrescente da versao dentro da chave de negocio.
- `event_count_for_key`: total de versoes distintas para a chave.

O ranking usa exatamente o tie-breaker documentado no projeto:

1. `transaction_datetime`
2. `transaction_date`
3. `ingestion_ts`
4. `record_hash`

## Flags de qualidade publicadas

Flags comuns em todas as fontes:

- `missing_business_key`
- `missing_transaction_datetime`
- `missing_transaction_date`
- `transaction_date_mismatch`

Flags especificas por fonte:

- `purchase`: status ausente ou invalido, valor monetario ausente ou negativo, `release_date` ausente, chave de `product_item` ausente, referencias faltantes para `product_item` e `purchase_extra_info`
- `product_item`: `product_id`, `item_quantity` ou `purchase_value` ausentes, quantidade nao positiva, valor negativo, orfandade em relacao a `purchase`
- `purchase_extra_info`: `subsidiary` ausente e orfandade em relacao a `purchase`
- `order_transaction_cost_hist`: data de custo ausente, valores de custo negativos e orfandade em relacao a `purchase`

O campo `quality_status` resume a linha como:

- `valid`: sem flags
- `warning`: flags nao bloqueantes
- `error`: problema bloqueante de chave de negocio ou ordenacao

## Como executar localmente

1. Carregar a bronze local com `make ingest-bronze`
2. Materializar a silver com `make build-silver`
3. Inspecionar as tabelas com `make pyspark`

Exemplo de consulta:

```sql
SELECT
    purchase_id,
    event_latest_rank,
    quality_status,
    quality_flags
FROM ae_challenge.silver_purchase_cdc
ORDER BY purchase_id, event_version_number;
```
