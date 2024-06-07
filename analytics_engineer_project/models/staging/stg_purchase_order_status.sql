WITH source AS (
	SELECT * FROM {{ source('northwind', 'purchase_order_status')}}
)
SELECT * FROM source