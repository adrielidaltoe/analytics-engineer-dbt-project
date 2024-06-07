WITH source AS (
	SELECT * FROM {{ source('northwind', 'suppliers')}}
)
SELECT * FROM source