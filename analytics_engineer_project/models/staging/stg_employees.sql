WITH source AS (
	SELECT * FROM {{ source('northwind', 'employees')}}
)
SELECT * FROM source