/*
Exercício 01

Premissas adotadas:
1. A data de reconhecimento da receita é `release_date`, pois representa a confirmação do pagamento.
2. Apenas compras com `purchase_status = 'APROVADA'` entram no faturamento.
3. Para faturamento por produtor, foi usado `purchase.purchase_total_value`.
4. Para faturamento por produto, o join entre `purchase` e `product_item` usa `prod_item_id` e `prod_item_partition`.
5. Foi assumido que `product_item.purchase_value` representa o valor da linha do item.
   Se no ambiente real esse campo representar valor unitário, a fórmula correta passa a ser:
   `item_quantity * purchase_value`.
6. A segunda pergunta não explicita recorte de tempo, então a consulta considera todo o histórico aprovado.
7. Antes das agregações, a consulta remove apenas duplicatas exatas no grão disponível.
   Isso evita inflar GMV sem assumir regras de desempate que não foram informadas.
*/

/* ============================================================
   1) 50 maiores produtores em faturamento em 2021
   ============================================================ */
WITH purchase_dedup AS (
    SELECT DISTINCT
        purchase_id,
        purchase_partition,
        producer_id,
        prod_item_id,
        prod_item_partition,
        purchase_total_value,
        purchase_status,
        release_date
    FROM
        purchase
),
approved_purchases_2021 AS (
    SELECT
        purchase_id,
        purchase_partition,
        producer_id,
        purchase_total_value
    FROM
        purchase_dedup
    WHERE
        purchase_status = 'APROVADA'
        AND release_date >= DATE '2021-01-01'
        AND release_date < DATE '2022-01-01'
)

SELECT
    producer_id,
    SUM(COALESCE(purchase_total_value, 0)) AS revenue_2021
FROM
    approved_purchases_2021
GROUP BY
    producer_id
ORDER BY
    revenue_2021 DESC,
    producer_id
LIMIT 50;


/* ============================================================
   2) 2 produtos com maior faturamento de cada produtor
   ============================================================ */
WITH purchase_dedup AS (
    SELECT DISTINCT
        purchase_id,
        purchase_partition,
        producer_id,
        prod_item_id,
        prod_item_partition,
        purchase_status
    FROM
        purchase
),

product_item_dedup AS (
    SELECT DISTINCT
        prod_item_id,
        prod_item_partition,
        product_id,
        purchase_value
    FROM
        product_item
),

approved_sales AS (
    SELECT
        purchase.producer_id,
        product_item.product_id,
        COALESCE(product_item.purchase_value, 0) AS item_revenue
    FROM
        purchase_dedup AS purchase
    INNER JOIN
        product_item_dedup AS product_item
            ON purchase.prod_item_id = product_item.prod_item_id
            AND purchase.prod_item_partition = product_item.prod_item_partition
    WHERE
        purchase.purchase_status = 'APROVADA'
),

product_revenue AS (
    SELECT
        producer_id,
        product_id,
        SUM(item_revenue) AS product_revenue
    FROM
        approved_sales
    GROUP BY
        producer_id,
        product_id
),

ranked_products AS (
    SELECT
        producer_id,
        product_id,
        product_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY producer_id
            ORDER BY product_revenue DESC, product_id
        ) AS product_rank
    FROM
        product_revenue
)

SELECT
    producer_id,
    product_id,
    product_revenue,
    product_rank
FROM
    ranked_products
WHERE
    product_rank <= 2
ORDER BY
    producer_id,
    product_rank,
    product_id;
