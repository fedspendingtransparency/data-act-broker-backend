SELECT
    row_number,
    availability_type_code
FROM object_class_program_activity
WHERE submission_id = {}
AND LOWER(COALESCE(availability_type_code,'')) NOT IN ('x','')

