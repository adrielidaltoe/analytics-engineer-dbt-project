WITH source AS (
	SELECT * FROM {{ source('northwind', 'inventory_transaction_types')}}
)
SELECT * FROM source