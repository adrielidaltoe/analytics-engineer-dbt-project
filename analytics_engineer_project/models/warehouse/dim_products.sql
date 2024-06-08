WITH dim_products AS (
	SELECT
		p.id AS product_id,
		p.product_code,
		p.product_name,
		p.description,
		s.company AS supplier_company,
		p.standard_cost,
		p.list_price,
		p.reorder_level,
		p.target_level,
		p.quantity_per_unit,
		p.discontinued,
		p.minimum_reorder_quantity,
		p.category,
		p.attachments,
		CURRENT_TIMESTAMP() AS insertion_timestamp
	FROM {{ ref('stg_products') }} AS p
	LEFT JOIN {{ ref('stg_suppliers') }} AS s
		ON p.supplier_id = s.id
),
unique_source AS (
	SELECT
		*,
		ROW_NUMBER() OVER (
			PARTITION BY product_id
		) AS row_number
	FROM dim_products
)
SELECT *
EXCEPT
	(row_number)
FROM unique_source
WHERE row_number = 1
ORDER BY product_id
