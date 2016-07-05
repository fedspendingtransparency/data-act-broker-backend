SELECT
    row_number,
    availability_type_code
FROM award_financial
WHERE submission_id = {}
AND LOWER(COALESCE(availability_type_code,'')) NOT IN ('x','')

