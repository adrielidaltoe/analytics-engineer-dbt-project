WITH source AS (
	SELECT * FROM {{ source('northwind', 'shippers')}}
)
SELECT
	*,
	CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source