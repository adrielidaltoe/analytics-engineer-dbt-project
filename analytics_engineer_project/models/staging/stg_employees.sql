WITH source AS (
	SELECT * FROM {{ source('northwind', 'employees')}}
)
SELECT
	*,
	CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source