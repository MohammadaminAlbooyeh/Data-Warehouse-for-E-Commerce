SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date AS date_id,
    o.quantity,
    o.total_amount
FROM {{ ref('stg_orders') }} o
