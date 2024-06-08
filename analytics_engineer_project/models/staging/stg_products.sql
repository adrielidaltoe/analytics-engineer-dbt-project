WITH source AS (
	SELECT
		id,
		CAST(supplier_ids AS INTEGER) AS supplier_id,
		product_code,
		product_name,
		description,
		standard_cost,
		list_price,
		reorder_level,
		target_level,
		quantity_per_unit,
		discontinued,
		minimum_reorder_quantity,
		category,
		attachments
	FROM {{ source('northwind', 'products')}}
	WHERE supplier_ids NOT LIKE '%;%'
)
SELECT
	*,
	CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source