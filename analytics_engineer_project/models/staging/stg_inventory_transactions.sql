WITH source AS (
	SELECT * FROM {{ source('northwind', 'inventory_transactions')}}
)
SELECT * FROM source