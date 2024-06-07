WITH source AS (
	SELECT * FROM {{ source('northwind', 'orders_tax_status')}}
)
SELECT * FROM source