WITH source AS (
	SELECT * FROM {{ source('northwind', 'orders')}}
)
SELECT * FROM source