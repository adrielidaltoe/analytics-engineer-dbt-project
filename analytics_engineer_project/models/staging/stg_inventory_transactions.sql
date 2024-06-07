WITH source AS (
	SELECT * FROM {{ source('northwind', 'inventory_transactions')}}
)
SELECT
	*,
	CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source