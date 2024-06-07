WITH source AS (
	SELECT * FROM {{ source('northwind', 'privileges')}}
)
SELECT * FROM source