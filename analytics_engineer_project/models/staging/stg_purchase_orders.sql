WITH source AS (
	SELECT * FROM {{ source('northwind', 'purchase_orders')}}
)
SELECT * FROM source