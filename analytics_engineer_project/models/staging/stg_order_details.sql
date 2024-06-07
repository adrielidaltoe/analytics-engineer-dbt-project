WITH source AS (
	SELECT * FROM {{ source('northwind', 'order_details')}}
)
SELECT * FROM source