SELECT
    approp.row_number,
    approp.availability_type_code,
    approp.beginning_period_of_availa,
    approp.ending_period_of_availabil
FROM appropriation AS approp
WHERE approp.submission_id = {}
    AND (approp.availability_type_code = 'x' OR approp.availability_type_code = 'X')
    AND (approp.beginning_period_of_availa IS NOT NULL OR approp.ending_period_of_availabil IS NOT NULL);