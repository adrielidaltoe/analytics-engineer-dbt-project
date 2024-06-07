WITH source AS (
	SELECT * FROM {{ source('northwind', 'order_details_status')}}
)
SELECT * FROM source