# Ingestao Raw da Camada Bronze

## Objetivo

Descrever como a camada bronze foi modelada e como o fluxo de ingestao utiliza os artefatos SQL e PySpark do projeto.

## Objetivos de desenho

- Preservar o historico raw dos eventos exatamente como chegaram ao lake bronze.
- Manter as tabelas bronze em modo append-only.
- Particionar as tabelas Iceberg por `transaction_date`.
- Adicionar metadados de ingestao necessarios para rastreabilidade e replay deterministico.
- Nao aplicar regras de negocio na bronze.
- Separar o carregamento generico da adaptacao da massa local de exemplo.

## Artefatos implementados

- DDL: [`sql/ddl/bronze_tables.sql`](../sql/ddl/bronze_tables.sql)
- Schemas de origem: [`src/common/source_schemas.py`](../src/common/source_schemas.py)
- Sessao Spark local: [`src/common/local_spark.py`](../src/common/local_spark.py)
- Execucao de arquivos SQL: [`src/common/sql.py`](../src/common/sql.py)
- Contratos da bronze: [`src/bronze/contracts.py`](../src/bronze/contracts.py)
- Escrita append-only: [`src/bronze/ingestion.py`](../src/bronze/ingestion.py)
- Loader generico da bronze: [`src/bronze/loader.py`](../src/bronze/loader.py)
- Adaptadores da massa de exemplo: [`src/bronze/sample_adapters.py`](../src/bronze/sample_adapters.py)
- Script de execucao local: [`scripts/ingest_bronze_local.py`](../scripts/ingest_bronze_local.py)

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

## Comportamento da ingestao

1. Executar o DDL da bronze para garantir namespace e tabelas.
2. Adaptar os dados de entrada para o schema canonico de cada origem usando [`source_schemas.py`](../src/common/source_schemas.py).
3. Validar se a origem suportada existe nos contratos da bronze.
4. Aplicar cast dos campos para o schema bronze da origem.
5. Calcular um `record_hash` deterministico com base no payload ordenado da origem.
6. Adicionar os metadados de ingestao.
7. Fazer append na tabela Iceberg de destino com `DataFrameWriterV2.writeTo(...).append()`.

## Separacao de responsabilidades

- [`src/bronze/loader.py`](../src/bronze/loader.py) contem somente o comportamento generico de preparo e carga.
- [`src/bronze/sample_adapters.py`](../src/bronze/sample_adapters.py) contem apenas a adaptacao da massa simplificada da pasta `data/` para o contrato do exercicio.
- [`scripts/ingest_bronze_local.py`](../scripts/ingest_bronze_local.py) existe apenas para facilitar a execucao local com a massa de exemplo.

## Ajustes feitos para a amostra local

- `purchase.txt` nao traz `purchase_partition`, `prod_item_partition`, `purchase_total_value` e `purchase_status`; a adaptacao local preenche os campos tecnicos de particao com valor fixo `1` e mantem os campos ausentes como `null`.
- `product_item.txt` usa `purchase_id` em vez de `prod_item_id`; a adaptacao local resolve `prod_item_id` a partir da amostra de `purchase`.
- `purchase_extra_info.txt` nao traz `purchase_partition`; a adaptacao local preenche esse campo com valor fixo `1`.
- `order_transaction_cost_hist.txt` e opcional no fluxo local. Se o arquivo nao existir, a carga segue sem essa origem.

## Observacoes

- Reenvios identicos continuam visiveis na bronze. A deduplicacao comeca na silver.
- Qualquer desvio nao tratado entre a entrada e o contrato esperado deve falhar cedo para ficar visivel nas verificacoes de qualidade e observabilidade.
- A adaptacao da massa local nao altera o comportamento generico do loader usado para a bronze.
