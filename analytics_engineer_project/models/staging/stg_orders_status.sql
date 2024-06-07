WITH source AS (
	SELECT * FROM {{ source('northwind', 'orders_status')}}
)
SELECT * FROM source