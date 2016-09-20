SELECT
    af.row_number,
    af.availability_type_code,
    af.beginning_period_of_availa,
    af.ending_period_of_availabil
FROM award_financial AS af
WHERE af.submission_id = {}
    AND (af.availability_type_code = 'x' OR af.availability_type_code = 'X')
    AND (af.beginning_period_of_availa IS NOT NULL OR af.ending_period_of_availabil IS NOT NULL);