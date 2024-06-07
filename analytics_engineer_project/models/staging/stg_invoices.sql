WITH source AS (
	SELECT * FROM {{ source('northwind', 'invoices')}}
)
SELECT * FROM source