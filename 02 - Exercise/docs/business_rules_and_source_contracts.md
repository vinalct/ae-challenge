# Regras de Negócio e Contratos das Fontes

## Objetivo

Definir as regras de negócio, os contratos das fontes, a estratégia de junção e a semântica histórica que devem guiar toda a solução.

## 1. Definições canônicas

- `transaction_datetime`: instante exato em que a versão do evento foi inserida no lake. É o principal campo de ordenação dentro do dia.
- `transaction_date`: data em que a versão do evento ficou disponível no lake. É o limite diário de disponibilidade e a partição física recomendada.
- `snapshot_date`: data de referência usada para reconstruir o que a plataforma conhecia ao final daquele dia.
- `gmv_date`: data de negócio para reconhecimento do GMV. É derivada de `release_date`.
- `current snapshot`: maior `snapshot_date` disponível na base histórica.

## 2. Regra canônica da métrica

### Definição de GMV

GMV é a soma do valor transacionado das compras cujo pagamento foi confirmado.

### Regra de elegibilidade

Uma compra é elegível para GMV em uma determinada `snapshot_date` somente quando todas as condições abaixo são verdadeiras no estado montado até aquela data:

- existe um registro de `purchase` para a chave da compra
- existe um registro de `product_item` para a chave do item referenciado
- existe um registro de `purchase_extra_info` para a chave da compra
- `purchase.release_date` não é nulo
- `purchase.purchase_status = 'APROVADA'`
- `purchase.purchase_total_value` não é nulo
- `purchase.purchase_total_value >= 0`
- a linha passou nas validações bloqueantes exigidas para elegibilidade na gold

### Regras explícitas de exclusão

Uma compra não é elegível para GMV em uma determinada `snapshot_date` quando qualquer uma das condições abaixo ocorre:

- `purchase_status = 'INICIADA'`
- `purchase_status = 'CANCELADA'`
- `purchase_status = 'REEMBOLSADA'`
- `release_date` é nulo
- `purchase_total_value` é nulo
- `purchase_total_value < 0`
- o `product_item` ou o `purchase_extra_info` obrigatório ainda não existe até aquela `snapshot_date`

### Regra do valor da métrica

- O valor de GMV vem de `purchase.purchase_total_value`.
- `product_item.purchase_value` é mantido para completude, linhagem e reconciliação, mas não é a fonte principal do GMV.
- `order_transaction_cost_hist` não entra no cálculo do GMV.

## 3. Semântica histórica

### Regra de snapshot

Para cada `snapshot_date`, os modelos downstream só podem usar eventos de origem com `transaction_date <= snapshot_date`.

Essa regra é a base da imutabilidade. Uma correção que chega depois pode alterar snapshots futuros, mas não pode reescrever uma `snapshot_date` já publicada.

### Eventos tardios e corretivos

- As tabelas de origem podem atualizar de forma assíncrona.
- Uma correção pode alterar um evento de negócio do passado.
- A correção só passa a valer a partir da `snapshot_date` em que ficou disponível no lake.
- Snapshots antigos permanecem inalterados porque não podem enxergar eventos com `transaction_date` futura.

### Interpretação do processamento diário

- O processamento roda em `D-1`.
- Se a plataforma roda no dia `D`, ela publica o estado histórico de `snapshot_date = D-1`.
- O snapshot representa o estado de fim de dia de todos os eventos disponíveis até aquela data.

## 4. Decisões de contrato por fonte

### 4.1 `purchase`

Propósito de negócio:

Define o ciclo de vida da compra e traz o valor e o status que dirigem o GMV.

Grão da entidade:

Um evento de estado de compra para uma combinação de `purchase_id` e `purchase_partition` em um determinado `transaction_datetime`.

Chave canônica da entidade:

- `purchase_id`
- `purchase_partition`

Campos relevantes:

- métricos: `purchase_total_value`, `purchase_status`, `release_date`
- contexto: `order_date`, `buyer_id`, `producer_id`
- junção com item: `prod_item_id`, `prod_item_partition`
- ordenação histórica: `transaction_datetime`, `transaction_date`

Regras do contrato:

- `purchase_id` é o identificador de negócio, mas joins downstream devem usar `purchase_id` junto com `purchase_partition` quando o schema exigir.
- `prod_item_id` e `prod_item_partition` são as chaves canônicas de junção com `product_item`.
- O evento válido mais recente até a `snapshot_date` define o estado ativo da compra naquele snapshot.

### 4.2 `product_item`

Propósito de negócio:

Traz os detalhes do item usados para completude e reconciliação.

Grão da entidade:

Um evento de estado do item para uma combinação de `prod_item_id` e `prod_item_partition` em um determinado `transaction_datetime`.

Chave canônica da entidade:

- `prod_item_id`
- `prod_item_partition`

Campos relevantes:

- reconciliação e descrição: `product_id`, `item_quantity`, `purchase_value`
- ordenação histórica: `transaction_datetime`, `transaction_date`

Regras do contrato:

- O schema escrito e as relações declaradas são a referência principal para os joins.
- O join de `purchase` com `product_item` é:
  - `purchase.prod_item_id = product_item.prod_item_id`
  - `purchase.prod_item_partition = product_item.prod_item_partition`
- Se algum print ou amostra mostrar `purchase_id` dentro de `product_item`, a implementação ainda deve normalizar para o schema escrito, porque as referências apontam explicitamente para `prod_item_id` e `prod_item_partition`.
- O evento válido mais recente até a `snapshot_date` define o estado ativo do item naquele snapshot.

### 4.3 `purchase_extra_info`

Propósito de negócio:

Traz a `subsidiary`, dimensão obrigatória para a agregação final.

Grão da entidade:

Um evento de estado de informação extra para uma combinação de `purchase_id` e `purchase_partition` em um determinado `transaction_datetime`.

Chave canônica da entidade:

- `purchase_id`
- `purchase_partition`

Campos relevantes:

- dimensão de negócio: `subsidiary`
- ordenação histórica: `transaction_datetime`, `transaction_date`

Regras do contrato:

- O join de `purchase` com `purchase_extra_info` é:
  - `purchase.purchase_id = purchase_extra_info.purchase_id`
  - `purchase.purchase_partition = purchase_extra_info.purchase_partition`
- O evento válido mais recente até a `snapshot_date` define a subsidiária ativa naquele snapshot.
- Como a saída final é por subsidiária, uma compra sem `purchase_extra_info` até a data de referência é considerada incompleta e não elegível para GMV.

### 4.4 `order_transaction_cost_hist`

Propósito de negócio:

Traz o histórico de custos transacionais para auditoria e rastreabilidade, mas não entra no cálculo de GMV.

Grão da entidade:

Um evento de histórico de custo para uma compra em uma determinada data efetiva de custo e um `transaction_datetime`.

Associação canônica:

- `purchase_id`
- `purchase_partition`

Campos relevantes:

- auditoria: `order_transaction_cost_vat_value`, `order_transaction_cost_installment_value`, `order_transaction_cost_date`
- ordenação histórica: `transaction_datetime`, `transaction_date`

Regras do contrato:

- O join de `purchase` com `order_transaction_cost_hist` é:
  - `purchase.purchase_id = order_transaction_cost_hist.purchase_id`
  - `purchase.purchase_partition = order_transaction_cost_hist.purchase_partition`
- `order_transaction_cost_hist` não é obrigatório para elegibilidade de GMV.
- O evento válido mais recente até a `snapshot_date` deve ser carregado apenas para linhagem e auditoria.

## 5. Contrato de junção

A estratégia canônica de join downstream é:

- `purchase` com `product_item` por `prod_item_id` e `prod_item_partition`
- `purchase` com `purchase_extra_info` por `purchase_id` e `purchase_partition`
- `purchase` com `order_transaction_cost_hist` por `purchase_id` e `purchase_partition`

### Grão da montagem downstream

- O grão do snapshot montado da compra é uma linha por `snapshot_date`, `purchase_id` e `purchase_partition`.
- `purchase` é a entidade âncora do snapshot de estado da compra.
- `product_item`, `purchase_extra_info` e `order_transaction_cost_hist` são anexados após a seleção do seu estado válido mais recente até a data de referência.

## 6. Seleção determinística do último evento

Para cada tabela de origem, o registro ativo até uma `snapshot_date` é escolhido pelas regras abaixo:

1. Filtrar a tabela para registros com `transaction_date <= snapshot_date`.
2. Remover apenas reenvios idênticos na silver.
3. Ordenar as versões restantes por:
   - `transaction_datetime` desc
   - `transaction_date` desc
   - `ingestion_ts` desc, quando os metadados da bronze existirem
   - `record_hash` desc como desempate determinístico final
4. Selecionar a linha de rank `1` como estado ativo daquela entidade no snapshot.

### Linhas inválidas para ordenação

- Linhas com chaves de negócio ausentes devem ser mantidas e sinalizadas para qualidade.
- Linhas com `transaction_datetime` ausente também devem ser sinalizadas.
- A seleção do snapshot gold só deve usar linhas que atendam aos requisitos mínimos de ordenação.

## 7. Regra de carry-forward entre fontes

Quando apenas uma tabela recebe um novo evento em um dia e as demais não recebem atualização, o estado montado da compra deve ser reconstruído usando:

- o evento válido mais novo da fonte atualizada
- o último estado ativo até a data de referência das fontes que não mudaram

Essa regra atende ao requisito de repetir o estado ativo das tabelas que não sofreram atualização.

## 8. Completude mínima para elegibilidade de GMV

Uma compra só pode aparecer na agregação final de GMV quando todos os componentes obrigatórios estiverem presentes no mesmo estado montado até a data de referência.

Obrigatórios para elegibilidade:

- `purchase`
- `product_item`
- `purchase_extra_info`

Não obrigatório para elegibilidade, mas rastreado:

- `order_transaction_cost_hist`

Flags de qualidade devem continuar visíveis mesmo quando a compra for excluída do GMV.

## 9. Regra de navegação histórica

A interpretação adotada é:

- consultar Jan/2023 com `snapshot_date = '2023-03-31'` hoje deve retornar a mesma resposta que seria retornada para essa mesma `snapshot_date` quando 31/03/2023 era o snapshot mais recente
- consultar Jan/2023 com a `snapshot_date` mais recente é uma pergunta diferente e pode refletir correções posteriores

Essa separação resolve o requisito histórico sem quebrar a imutabilidade.

## 10. Exemplo com a ilustração do desafio

Usando o comportamento mostrado no enunciado:

- a compra `55` aparece pela primeira vez em `2023-01-20`
- o `purchase_extra_info` dessa compra chega apenas em `2023-01-23`
- uma correção posterior de `product_item` chega em `2023-07-12`
- uma correção posterior de `purchase` chega em `2023-07-15`

Resultado:

- snapshots antes de `2023-01-23` não possuem estado completo da compra para publicação final do GMV, porque a `subsidiary` ainda está ausente
- snapshots a partir de `2023-01-23` podem usar o último estado ativo conhecido
- snapshots antes de `2023-07-12` e `2023-07-15` preservam o entendimento anterior de janeiro
- snapshots em `2023-07-12`, `2023-07-15` e posteriores podem refletir os valores corrigidos

## 11. Premissas e ponto em aberto

### Premissas adotadas

- O schema escrito e as referências declaradas têm precedência sobre os prints quando houver conflito.
- `purchase.purchase_total_value` é a fonte do valor de GMV.
- `transaction_date` é o limite diário oficial de disponibilidade.
- `purchase_partition` e `prod_item_partition` fazem parte das chaves canônicas quando exigido pelas referências.
- Valores aprovados negativos são tratados como falha de qualidade e ficam fora do GMV até correção.

### Ponto em aberto não bloqueante

- Se a origem persistir `product_item` com `purchase_id` em vez de `prod_item_id`, a silver precisará de uma etapa explícita de normalização antes de aplicar o contrato canônico de join. Isso não altera o contrato alvo descrito aqui.
