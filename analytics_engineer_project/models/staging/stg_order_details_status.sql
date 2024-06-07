WITH source AS (
	SELECT * FROM {{ source('northwind', 'order_details_status')}}
)
SELECT
	*,
	CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source