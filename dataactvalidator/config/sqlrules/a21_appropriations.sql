SELECT
    row_number,
    availability_type_code
FROM appropriation
WHERE submission_id = {}
AND LOWER(COALESCE(availability_type_code,'')) NOT IN ('x','')

