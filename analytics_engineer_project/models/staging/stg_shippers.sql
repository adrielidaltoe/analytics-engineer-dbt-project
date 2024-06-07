WITH source AS (
	SELECT * FROM {{ source('northwind', 'shippers')}}
)
SELECT * FROM source