SELECT
    row_number,
    availability_type_code,
    beginning_period_of_availa,
    ending_period_of_availabil
FROM appropriation
WHERE submission_id = {}
    AND LOWER(availability_type_code) = 'x'
    AND (beginning_period_of_availa IS NOT NULL OR ending_period_of_availabil IS NOT NULL);