WITH source AS (
	SELECT * FROM {{ source('northwind', 'employee_privileges')}}
)
SELECT * FROM source