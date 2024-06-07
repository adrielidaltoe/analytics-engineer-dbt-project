WITH source AS (
	SELECT * FROM {{ source('northwind', 'products')}}
)
SELECT * FROM source