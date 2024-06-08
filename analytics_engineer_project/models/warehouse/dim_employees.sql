WITH dim_employees AS (
	SELECT
		id AS employee_id,
		company,
		last_name,
		first_name,
		email_address,
		job_title,
		business_phone,
		home_phone,
		mobile_phone,
		fax_number,
		address,
		city,
		state_province,
		zip_postal_code,
		country_region,
		web_page,
		notes,
		attachments,
		CURRENT_TIMESTAMP() AS insertion_timestamp
	FROM {{ ref('stg_employees') }}
),
unique_source AS (
	SELECT
		*,
		ROW_NUMBER() OVER (
			PARTITION BY employee_id
		) AS row_number
	FROM dim_employees
)
SELECT *
EXCEPT
	(row_number)
FROM unique_source
WHERE row_number = 1
ORDER BY employee_id
