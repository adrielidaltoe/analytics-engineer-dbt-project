WITH source AS (
	SELECT * FROM {{ source('northwind', 'products')}}
)
SELECT
	*,
	CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source