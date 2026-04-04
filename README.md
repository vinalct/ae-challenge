# Analytics Engineering Challenge

Este repositório contém a solução completa de um desafio dividido em dois exercícios.

A proposta do desafio não é apenas responder perguntas em SQL. Ele também pede a construção de um produto de dados histórico, reprodutível e fácil de revisar, baseado em eventos CDC. Este `README.md` é o ponto único de entrada para entender o projeto inteiro.

## Objetivo do Projeto

O repositório foi organizado para atender dois objetivos diferentes:

- no Exercício 01, responder perguntas analíticas diretamente em SQL
- no Exercício 02, modelar e implementar uma pipeline histórica de GMV diário por subsidiária usando PySpark e Apache Iceberg

## Resumo do Desafio

O desafio tem duas partes complementares:

- uma parte de análise SQL sobre o modelo de dados fornecido
- uma parte de modelagem histórica com ETL, camada analítica final, query de consumo e documentação para revisão

Para o Exercício 02, os entregáveis pedidos pelo desafio são:

- Script do ETL - (preferencialmente em python, spark ou scala)
- Create table do dataset final - (DDL)
- Um exemplo da tabela final populada
- A query SQL para obter o GMV diário por subsidiária usando o estado atual
- A descrição da stack técnica adotada
- Documentação clara para quem for revisar a solução (nao solicitada, mas entregue)

## Estrutura do Repositório

```text
.
├── 01 - Exercise/
│   └── solution.sql
├── 02 - Exercise/
│   ├── Makefile
│   ├── Dockerfile
│   ├── data/
│   ├── docs/
│   ├── examples/
│   ├── scripts/
│   ├── sql/
│   ├── src/
│   └── tests/
└── README.md
```

## Caminho Rápido para Revisão

Se a ideia for revisar a solução rapidamente, a sequência recomendada é:

1. Ler este `README.md`. Ele foi escrito para ser suficiente como visão geral da solução.
2. Abrir [solution.sql](./01%20-%20Exercise/solution.sql) para ver a resposta do Exercício 01.
3. No Exercício 02, ler as seções `Como a Solução Atende aos Pré-Requisitos`, `Qualidade de Dados, Confiabilidade e Idempotência` e `Mapa de Entregáveis` neste próprio `README.md`.
4. Usar os documentos da pasta [docs](./02%20-%20Exercise/docs) apenas se quiser aprofundar algum ponto específico.
5. Se necessário, executar localmente os comandos descritos na seção `Como Executar Localmente`.

## Exercício 01

### Propósito

O Exercício 01 é a parte SQL do desafio. Ele responde duas perguntas analíticas sobre o modelo transacional fornecido.

### Perguntas Respondidas

- Quais são os 50 maiores produtores em faturamento aprovado em 2021?
- Quais são os 2 produtos de maior faturamento para cada produtor?

### Entregável Principal

- SQL final: [solution.sql](./01%20-%20Exercise/solution.sql)

### Premissas Adotadas

As premissas estão documentadas no topo do próprio arquivo SQL. As principais são:

- o reconhecimento de receita usa `release_date`
- apenas compras com `purchase_status = 'APROVADA'` entram no faturamento
- o faturamento por produtor usa `purchase.purchase_total_value`
- o faturamento por produto usa o join entre `purchase` e `product_item` por `prod_item_id` e `prod_item_partition`

## Exercício 02

### Propósito

O Exercício 02 implementa um produto de dados histórico que responde, de forma reprodutível:

- qual era o GMV diário por subsidiária em uma determinada `snapshot_date`
- qual é a resposta atual usando o snapshot mais recente disponível

### Interpretação de Negócio

A solução foi desenhada com as seguintes premissas de negócio:

- o GMV vem de `purchase.purchase_total_value`
- uma compra só é elegível quando o estado mais recente até o snapshot está aprovado e completo
- eventos tardios e correções afetam snapshots futuros, nunca reescrevem snapshots antigos
- a saída final precisa preservar a diferença entre data de conhecimento e data de negócio

Por isso, o modelo final guarda as duas dimensões temporais:

- `snapshot_date`: o que a plataforma conhecia naquela data
- `gmv_date`: a data de negócio do GMV, derivada de `release_date`

### Arquitetura Implementada

A solução foi construída no padrão medallion:

- `bronze`: ingestão raw imutável com metadados de ingestão
- `silver`: padronização CDC, deduplicação de reenvios idênticos e flags de qualidade
- `gold` camada 1: `gold_purchase_state_snapshot`
- `gold` camada 2: `gold_gmv_daily_by_subsidiary_snapshot`
- `ops`: auditoria operacional, resultados de qualidade e quarantine

### Como a Solução Atende aos Pré-Requisitos do Exercício 02

- interpretação dos eventos: a solução assume explicitamente a diferença entre `transaction_date`, `snapshot_date` e `gmv_date`, e monta a métrica a partir dessa semântica histórica
- dados faltantes e inconsistentes: a pipeline não ignora problemas silenciosamente; ela publica flags de qualidade, resultados de checks e quarantine
- todas as tabelas são gatilhos: mudanças em `purchase`, `product_item`, `purchase_extra_info` e `order_transaction_cost_hist` podem alterar o estado montado e, portanto, o resultado final
- carry-forward entre fontes: se só uma tabela mudar em um dia, o snapshot repete o último estado conhecido das demais
- atualização em `D-1`: o modo incremental publica `D-1` a partir de `PROCESS_DATE`
- passado imutável: cada `snapshot_date` só enxerga eventos com `transaction_date <= snapshot_date`, então correções futuras não reescrevem snapshots antigos
- navegação histórica estável: consultar uma mesma `snapshot_date` histórica hoje retorna a mesma resposta que essa data teria quando era corrente
- rastreabilidade diária: o snapshot mantém granularidade diária e guarda linhagem das fontes por registro contribuinte
- partição: bronze e silver usam `transaction_date`; as tabelas históricas finais usam `snapshot_date`, que é a dimensão natural de consulta do histórico publicado
- acesso ao estado corrente: a tabela histórica permite recuperar facilmente o snapshot mais recente por `MAX(snapshot_date)`, e o repositório também entrega a SQL pronta para isso
- stack pedida: a implementação foi feita em Python com PySpark e Apache Iceberg
- select final de consumo: o repositório entrega a query para ler o GMV corrente diretamente da base histórica

### Qualidade de Dados, Confiabilidade e Idempotência

Como o enunciado destaca tratamento da qualidade dos dados como critério forte de avaliação, a solução do Exercício 02 trata qualidade como parte do produto de dados, e não como uma checagem isolada no fim do fluxo.

O padrão implementado é confiável por cinco motivos principais:

- a qualidade é validada em múltiplas camadas, e não apenas na saída final
- regras bloqueantes e não bloqueantes são separadas por severidade, evitando aprovar silenciosamente dados inválidos
- as anomalias não são "apagadas" sem rastreio: elas viram flags, resultados de check e amostras em quarantine
- a gold reconcilia a métrica final contra o snapshot elegível, reduzindo o risco de divergência entre regra de negócio e agregado publicado
- a solução preserva linhagem até os eventos de origem e possui testes unitários, de integração e de data quality

Na prática, a qualidade foi desenhada assim:

- `bronze`: valida contrato de schema, completude de `transaction_date`, taxa de reenvio bruto e freshness
- `silver`: padroniza strings, remove resend duplicado exato, controla versionamento CDC e publica flags para chaves faltantes, valores inválidos e referências órfãs
- `gold purchase snapshot`: garante unicidade por grão, evita future leakage e recompõe `is_metric_eligible` a partir das colunas publicadas
- `gold GMV`: reconcilia o agregado com o snapshot elegível, garante unicidade do grão analítico e monitora anomalias de variação
- `ops`: publica `ops_pipeline_run_log`, `ops_data_quality_results` e `ops_data_quality_quarantine`, permitindo auditoria operacional com evidência concreta do que falhou

Esse desenho é confiável porque combina prevenção, detecção e rastreabilidade:

- prevenção: contratos e regras de elegibilidade já na transformação
- detecção: checks bloqueantes e warnings em bronze, silver e gold
- rastreabilidade: linhagem de source records, business keys impactadas e payload serializado em quarantine

### Idempotência da Pipeline

A resposta correta aqui é: a pipeline é idempotente nas saídas analíticas publicadas, mas não é 100% idempotente em todas as tabelas do repositório.

O que é idempotente hoje:

- `silver` e `gold` são publicados de forma determinística para a mesma entrada
- em `MODE=full-refresh`, as tabelas analíticas são recriadas/republicadas integralmente
- em `MODE=incremental`, a publicação reprocessa apenas `D-1` e sobrescreve o recorte impactado, em vez de acumular resultados na saída analítica
- com isso, repetir o mesmo processamento para a mesma janela produz o mesmo estado final nas tabelas analíticas consumidas

O que não é estritamente idempotente por desenho:

- `bronze` é append-only raw. Se a mesma ingestão incremental for executada novamente, os mesmos eventos brutos podem ser reapendados
- `ops` também é append-only histórico. Cada execução de auditoria gera um novo `run_id` e preserva o histórico dos runs anteriores

Mesmo com essa nuance, a modelagem atende bem o objetivo do desafio porque o consumo analítico final é reprocessável, determinístico e auditável. Em outras palavras: a parte que responde à métrica de negócio foi construída para ser confiável em reruns; as camadas raw e de auditoria preservam histórico operacional por design.

### Documentos Técnicos para Aprofundamento Opcional

- Regras de negócio e contratos das fontes: [business_rules_and_source_contracts.md](./02%20-%20Exercise/docs/business_rules_and_source_contracts.md)
- Ingestão bronze: [bronze_raw_ingestion.md](./02%20-%20Exercise/docs/bronze_raw_ingestion.md)
- Padronização silver: [silver_cdc_standardization.md](./02%20-%20Exercise/docs/silver_cdc_standardization.md)
- Snapshot gold de compras: [gold_purchase_state_snapshot.md](./02%20-%20Exercise/docs/gold_purchase_state_snapshot.md)
- Agregado final de GMV: [gold_gmv_daily_by_subsidiary.md](./02%20-%20Exercise/docs/gold_gmv_daily_by_subsidiary.md)
- Qualidade e observabilidade: [data_quality_and_observability.md](./02%20-%20Exercise/docs/data_quality_and_observability.md)

## Mapa de Entregáveis do Exercício 02

Abaixo está o mapeamento direto entre o que o desafio pede e os arquivos que atendem cada item.

| Requisito do desafio | Artefato no repositório |
| --- | --- |
| ETL em PySpark | [src/bronze](./02%20-%20Exercise/src/bronze), [src/silver](./02%20-%20Exercise/src/silver), [src/gold](./02%20-%20Exercise/src/gold), [src/ops](./02%20-%20Exercise/src/ops) |
| DDL da tabela final histórica | [gold_gmv_daily_by_subsidiary_snapshot.sql](./02%20-%20Exercise/sql/ddl/gold_gmv_daily_by_subsidiary_snapshot.sql) |
| SQL da view de acesso ao snapshot atual | [vw_gmv_daily_by_subsidiary_current.sql](./02%20-%20Exercise/sql/access/vw_gmv_daily_by_subsidiary_current.sql) |
| Query SQL para consumo atual do GMV diário por subsidiária | [select_current_gmv_daily_by_subsidiary.sql](./02%20-%20Exercise/sql/access/select_current_gmv_daily_by_subsidiary.sql) |
| Exemplo da tabela final populada | [gold_gmv_daily_by_subsidiary_snapshot_example.csv](./02%20-%20Exercise/examples/gold_gmv_daily_by_subsidiary_snapshot_example.csv) |
| Regras de negócio e interpretação da métrica | [business_rules_and_source_contracts.md](./02%20-%20Exercise/docs/business_rules_and_source_contracts.md) |
| Explicação da camada final e do consumo atual | [gold_gmv_daily_by_subsidiary.md](./02%20-%20Exercise/docs/gold_gmv_daily_by_subsidiary.md) |
| Observabilidade e qualidade de dados | [data_quality_and_observability.md](./02%20-%20Exercise/docs/data_quality_and_observability.md) |

## Stack Técnica Adotada

A stack implementada no Exercício 02 foi:

- processamento: PySpark
- formato de tabela: Apache Iceberg
- arquitetura de modelagem: medallion (`bronze` -> `silver` -> `gold`)
- validação automatizada: `pytest` + `chispa`
- outputs operacionais: tabelas `ops` para status de pipeline, resultados de qualidade e quarantine
- execução local: container com Python, Java e Spark, via Docker ou Podman

### Por que PySpark

PySpark foi escolhido porque o desafio exige reconstrução histórica baseada em eventos CDC, com joins, regras temporais e reprocessamento completo quando necessário. Ele resolve bem:

- ordenação determinística de eventos
- agregações e joins em escala
- rebuilds completos com lógica histórica reproduzível
- separação clara das camadas da pipeline

### Por que Apache Iceberg

Iceberg foi escolhido porque a tabela final precisa ser histórica, imutável por `snapshot_date` e segura para reprocessamento. Ele ajuda com:

- operações ACID sobre as tabelas
- partição por datas de snapshot
- publicação consistente dos datasets finais
- modelagem natural para histórico analítico

### O que foi mantido fora do escopo do repositório

O desafio menciona componentes operacionais complementares, mas eles foram apenas descritos, não implementados aqui:

- orquestração com Airflow, Dagster ou Databricks Workflows
- CI com GitHub Actions
- notificações para data owners via Slack ou email
- frameworks gerenciados de expectativas, como Great Expectations ou Deequ

## Exemplo da Saída Final Entregue

O arquivo [gold_gmv_daily_by_subsidiary_snapshot_example.csv](./02%20-%20Exercise/examples/gold_gmv_daily_by_subsidiary_snapshot_example.csv) foi montado como um recorte histórico intencional da tabela final para mostrar, no próprio artefato entregue, os cenários mais importantes da solução.

Esse CSV demonstra de forma objetiva:

- o grão da saída final: uma linha por `snapshot_date`, `gmv_date` e `subsidiary`
- a presença de histórico diário por `snapshot_date`
- a compra `55` entrando no GMV somente após a chegada de `purchase_extra_info`
- a compra `69` entrando no GMV quando a informação complementar passa a existir
- a mudança de `subsidiary` da compra `69` entre `2023-03-11` e `2023-03-12`
- a navegação histórica em `2023-03-31`, com estado coerente para os valores de janeiro e fevereiro
- o efeito do reembolso da compra `55`, que deixa de aparecer no snapshot de `2023-07-15`
- o formato final que o consumidor recebe, com métricas, status de qualidade e `snapshot_created_at`

Com isso, o exemplo entregue não mostra apenas o layout da tabela: ele também evidencia, no próprio CSV, chegada tardia de dados, carry-forward, mudança posterior de dimensão e preservação do histórico publicado por `snapshot_date`.

## Como Executar Localmente

Toda a execução local do Exercício 02 acontece dentro da pasta `02 - Exercise`.

### Pré-requisitos

- um runtime de container compatível com Docker CLI, como Docker ou Podman
- `make`
- acesso à internet no primeiro uso do Spark com Iceberg, para baixar o runtime necessário

### Comandos Principais

```bash
cd "02 - Exercise"
make image
make compile
make build-gold-gmv-daily-by-subsidiary MODE=full-refresh
make build-gold-gmv-daily-by-subsidiary MODE=incremental PROCESS_DATE=2026-04-04
make run-data-quality
make test
```

### O que cada comando faz

- `make image`: cria a imagem local com Python, Java e PySpark
- `make compile`: valida os módulos Python com `python -m compileall`
- `make build-gold-gmv-daily-by-subsidiary MODE=full-refresh`: reconstrói bronze, silver, snapshot gold e agregado final de GMV
- `make build-gold-gmv-daily-by-subsidiary MODE=incremental PROCESS_DATE=YYYY-MM-DD`: publica apenas D-1 em todas as camadas já materializadas
- `make run-data-quality`: publica as tabelas de auditoria da camada `ops` e falha se houver regra bloqueante
- `make test`: executa os testes PySpark unitários, de integração e de qualidade
- `make pyspark`: abre um console Spark interativo já configurado com o catálogo local


### Modos de Carga

- `MODE=full-refresh`: recria e republica a pipeline inteira desde a bronze.
- `MODE=incremental`: processa apenas `D-1`, calculado a partir de `PROCESS_DATE`.
- se `PROCESS_DATE` não for informado, o incremental assume a data atual do ambiente e publica o dia anterior.


### Observação sobre a view atual no catálogo local

O catálogo Iceberg local usado no exercício não suporta views persistidas. Por isso:

- a definição da view continua presente como entregável do desafio
- para ler a resposta atual localmente, a query recomendada é [select_current_gmv_daily_by_subsidiary.sql](./02%20-%20Exercise/sql/access/select_current_gmv_daily_by_subsidiary.sql)

## Testes

A suíte de testes do Exercício 02 valida a lógica das transformações e os contratos de qualidade usando DataFrames pequenos e determinísticos em memória.

Principais arquivos de teste:

- [test_silver_standardization.py](./02%20-%20Exercise/tests/unit/test_silver_standardization.py)
- [test_gold_purchase_snapshot.py](./02%20-%20Exercise/tests/unit/test_gold_purchase_snapshot.py)
- [test_gold_gmv_daily.py](./02%20-%20Exercise/tests/unit/test_gold_gmv_daily.py)
- [test_observability_pipeline.py](./02%20-%20Exercise/tests/integration/test_observability_pipeline.py)
- [test_ops_quality_outputs.py](./02%20-%20Exercise/tests/data_quality/test_ops_quality_outputs.py)

A explicação de por que os testes usam massas pequenas e hardcoded está em [data_quality_and_observability.md](./02%20-%20Exercise/docs/data_quality_and_observability.md).

## O que Está Implementado e o que Está Apenas Descrito

Implementado neste repositório:

- a resposta SQL do Exercício 01
- a pipeline medallion em PySpark do Exercício 02
- os DDLs finais em Iceberg
- os artefatos SQL de consumo atual
- um exemplo populado da saída final
- testes unitários, de integração e de qualidade
- saídas operacionais de qualidade e observabilidade

Apenas descrito como complemento operacional:

- orquestração
- CI
- notificações para data owners
- suites externas de expectativa e monitoramento
