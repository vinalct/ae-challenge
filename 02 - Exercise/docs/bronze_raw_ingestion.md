# Ingestão Raw da Camada Bronze

## Objetivo

Descrever como a camada bronze foi modelada e como o código de ingestão em PySpark deve ser utilizado.

## Objetivos de desenho

- Preservar o histórico raw dos eventos exatamente como chegaram.
- Manter as tabelas bronze em modo append-only.
- Particionar as tabelas Iceberg por `transaction_date`.
- Adicionar metadados de ingestão necessários para rastreabilidade e replay determinístico.
- Não aplicar regras de negócio na bronze.

## Artefatos implementados

- DDL: [`sql/ddl/bronze_tables.sql`](../sql/ddl/bronze_tables.sql)
- Schemas de origem: [`src/common/source_schemas.py`](../src/common/source_schemas.py)
- Contratos da bronze: [`src/bronze/contracts.py`](../src/bronze/contracts.py)
- Lógica de ingestão: [`src/bronze/ingestion.py`](../src/bronze/ingestion.py)

## Tabelas bronze

- `bronze_purchase_events`
- `bronze_product_item_events`
- `bronze_purchase_extra_info_events`
- `bronze_order_transaction_cost_hist_events`

## Metadados adicionados na bronze

- `ingestion_ts`
- `batch_id`
- `source_file`
- `record_hash`

## Comportamento da ingestão

1. Validar se o dataframe recebido possui o schema esperado para a origem.
2. Aplicar cast dos campos para o schema bronze daquela fonte.
3. Calcular um `record_hash` determinístico com base no payload ordenado da origem.
4. Adicionar os metadados de ingestão.
5. Fazer append na tabela Iceberg de destino com `DataFrameWriterV2.writeTo(...).append()`.

## Observações

- Reenvios idênticos continuam visíveis na bronze. A deduplicação começa na silver.
- Qualquer desvio de schema deve falhar cedo para ficar visível nas verificações de qualidade e observabilidade.
- Campos monetários permanecem próximos da representação original na bronze. Ajustes de precisão podem ocorrer depois na silver e na gold.
