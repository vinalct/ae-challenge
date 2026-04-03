# Data Quality e Observabilidade

## Objetivo

Descrever os checks implementados para bronze, silver e gold, os artefatos operacionais publicados na camada `ops` e o contrato de notificacao esperado para o dono do dado.

## Escopo implementado

- testes PySpark com `pytest` e `chispa`
- tabelas operacionais Iceberg para status de pipeline, resultados de qualidade e quarantine
- regras bloqueantes e nao bloqueantes para bronze, silver e gold
- amostras de registros falhos com chaves impactadas
- documentacao do payload de alerta e dos complementos operacionais nao implementados aqui

## Tabelas OPS

- `ops_pipeline_run_log`: status por tabela e um resumo geral da execucao
- `ops_data_quality_results`: resultado de cada regra com severidade, contagem impactada, threshold e chaves afetadas
- `ops_data_quality_quarantine`: amostra de registros falhos serializados em JSON

Essas tabelas sao criadas por [`sql/ddl/ops_tables.sql`](../sql/ddl/ops_tables.sql) e materializadas pelo runner [`scripts/run_data_quality_local.py`](../scripts/run_data_quality_local.py).

## Severidade das regras

- `error`: check bloqueante. O runner local publica os artefatos OPS e termina com erro para interromper a pipeline.
- `warning`: check nao bloqueante. O runner publica os resultados e termina com sucesso, mas o `run_status` fica como `completed_with_warnings`.

## Regras implementadas

### Bronze

- `bronze_schema_contract_match`: valida schema esperado por tabela
- `bronze_partition_key_completeness`: exige `transaction_date` nao nulo
- `bronze_raw_duplicate_rate`: monitora reenvios identicos por `record_hash`
- `bronze_freshness_lag_days`: mede lag de `transaction_date` contra a tabela bronze mais fresca

### Silver

- `silver_blocking_quality_status`: exige chaves de negocio e colunas de ordenacao validas
- `silver_post_dedup_exact_resend_uniqueness`: garante ausencia de duplicidade residual por `business_key + record_hash`
- `silver_purchase_status_contract`: valida enum de `purchase_status`
- `silver_negative_amount_contract`: bloqueia valores monetarios negativos
- `silver_orphan_reference_contract`: monitora referencias orfas entre as entidades

### Gold purchase snapshot

- `gold_snapshot_uniqueness_by_grain`: garante unicidade por `snapshot_date`, `purchase_id`, `purchase_partition`
- `gold_snapshot_no_future_leakage`: bloqueia uso de eventos com data posterior ao snapshot
- `gold_snapshot_completeness_flag_contract`: valida coerencia dos flags `has_*`
- `gold_snapshot_metric_eligibility_contract`: recomputa `is_metric_eligible` a partir das colunas publicadas

### Gold GMV aggregate

- `gold_gmv_uniqueness_by_grain`: garante unicidade por `snapshot_date`, `gmv_date`, `subsidiary`
- `gold_gmv_reconciliation_to_snapshot`: reconcilia o agregado com o snapshot elegivel
- `gold_gmv_non_negative_amounts`: impede GMV negativo
- `gold_gmv_daily_swing_anomaly`: publica warning para swings diarios acima do threshold configurado

## Testes do repositorio

Os testes ficam em:

- `tests/unit/`: transformacoes silver e gold
- `tests/integration/`: fluxo integrado ate as saidas de observabilidade
- `tests/data_quality/`: comportamento da camada OPS e quarantine

Esses testes nao usam os arquivos `data/*.txt` como massa principal. A intencao da suite e validar a logica da pipeline e os contratos definidos, e nao congelar o comportamento exato da massa de exemplo usada no demo local.

Em vez disso, cada teste monta DataFrames pequenos e deterministas em memoria. O fixture [`tests/conftest.py`](../tests/conftest.py) cria uma `SparkSession` local e oferece o helper `bronze_df_builder`, que constroi DataFrames com o schema canonico e com os metadados bronze esperados, como `ingestion_ts`, `batch_id`, `source_file` e `record_hash`.

Esse desenho foi escolhido para:

- isolar uma regra de negocio por teste
- deixar o cenario legivel e pequeno
- evitar dependencia de arquivos locais, Iceberg ou estado previo da pipeline
- modelar facilmente casos de borda, como resend duplicado, correcao tardia e quebra de contrato

Na pratica, o que esta sendo validado e:

- [`tests/unit/test_silver_standardization.py`](../tests/unit/test_silver_standardization.py): deduplicacao CDC, preservacao de correcoes, versionamento e `event_latest_rank`
- [`tests/unit/test_gold_purchase_snapshot.py`](../tests/unit/test_gold_purchase_snapshot.py): carry-forward diario, joins as-of e elegibilidade metrica no snapshot
- [`tests/unit/test_gold_gmv_daily.py`](../tests/unit/test_gold_gmv_daily.py): filtro por `is_metric_eligible`, agregacao diaria, contagens, quantidade e MTD
- [`tests/integration/test_observability_pipeline.py`](../tests/integration/test_observability_pipeline.py): fluxo minimo bronze -> silver -> gold -> ops com caso consistente
- [`tests/data_quality/test_ops_quality_outputs.py`](../tests/data_quality/test_ops_quality_outputs.py): falha de regra, publicacao em `ops_data_quality_results` e amostragem em quarantine

O que a suite nao cobre diretamente:

- a massa literal atual de `data/*.txt` como "golden dataset"
- persistencia Iceberg como parte de cada teste PySpark
- o fluxo inteiro do container como um teste de regressao fim a fim

Essas partes sao exercitadas pelos comandos locais do projeto, como `make ingest-bronze`, `make build-gold-gmv-daily-by-subsidiary` e `make run-data-quality`. Ja a pasta `tests/` e executada apenas por descoberta do `pytest`, via `make test`.

Comando local:

```bash
make test
```

## Como executar a auditoria local

```bash
make build-gold-gmv-daily-by-subsidiary
make run-data-quality
```

Se houver falha bloqueante, o comando termina com erro depois de publicar as tabelas OPS. Isso permite inspecionar os resultados sem depender apenas de logs.

Exemplos de consulta:

```sql
SELECT *
FROM ae_challenge.ops_pipeline_run_log
ORDER BY finished_at DESC, layer_name, table_name;

SELECT *
FROM ae_challenge.ops_data_quality_results
WHERE rule_status = 'failed'
ORDER BY evaluated_at DESC, layer_name, table_name, rule_name;

SELECT *
FROM ae_challenge.ops_data_quality_quarantine
ORDER BY captured_at DESC, layer_name, table_name, rule_name;
```

## Contrato de notificacao para o dono do dado

Entrega de alerta nao e implementada neste repositorio, mas o payload esperado deve incluir pelo menos:

- `run_id`
- `pipeline_name`
- `layer_name`
- `table_name`
- `rule_name`
- `severity`
- `rule_status`
- `transaction_date` ou `snapshot_date` afetado, quando existir
- `impacted_record_count`
- `impacted_business_keys`

Roteamento esperado:

- Slack para visibilidade rapida de incidentes e warnings
- email para trilha duravel de auditoria e distribuicao formal ao owner

## Complementos operacionais nao implementados aqui

O repositorio documenta a expectativa, mas nao implementa:

- orquestracao com Airflow, Dagster ou Databricks Workflows
- entrega automatica de alertas em Slack ou email
- CI com GitHub Actions
- suites gerenciadas com Great Expectations ou Deequ

## Relacao com a linhagem ja publicada

A linhagem de origem ate gold continua exposta nas colunas de source lineage do snapshot [`gold_purchase_state_snapshot`](../sql/ddl/gold_purchase_state_snapshot.sql). A camada `ops` complementa essa rastreabilidade com resultados de checks, contagens e amostras falhas.
