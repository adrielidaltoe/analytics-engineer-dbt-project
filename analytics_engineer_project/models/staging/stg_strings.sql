WITH source AS (
	SELECT * FROM {{ source('northwind', 'strings')}}
)
SELECT * FROM source