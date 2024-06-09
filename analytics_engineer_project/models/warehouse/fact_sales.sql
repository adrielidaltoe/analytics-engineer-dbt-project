{{ config(
	partition_by={
		"field": "order_date",
		"data_type": "date"
	}
)}}

WITH fact_sales AS (
	SELECT
		od.order_id,
		od.product_id,
		o.customer_id,
		o.employee_id,
		o.shipper_id,
		od.quantity,
		od.unit_price,
		od.discount,
		od.status_id,
		od.date_allocated,
		od.purchase_order_id,
		od.inventory_id,
		DATE(o.order_date) AS order_date,
		o.shipped_date,
		o.paid_date,
		CURRENT_TIMESTAMP() AS insertion_timestamp
	FROM {{ ref('stg_orders') }} AS o
	LEFT JOIN {{ ref('stg_order_details') }} AS od
		ON o.id = od.order_id
	WHERE od.order_id IS NOT NULL
),
unique_source AS (
	SELECT
		*,
		ROW_NUMBER() OVER (
			PARTITION BY order_id, product_id, customer_id, employee_id,
						 shipper_id, purchase_order_id, order_date
		) AS row_number
	FROM fact_sales
)
SELECT *
EXCEPT
	(row_number)
FROM unique_source
WHERE row_number = 1
