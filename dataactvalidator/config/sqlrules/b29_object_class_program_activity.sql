-- For File B PriorYearAdjustment must be one of X, B, or P.
SELECT
    row_number,
    prior_year_adjustment,
    display_tas AS "uniqueid_TAS"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND UPPER(prior_year_adjustment) NOT IN ('X', 'B', 'P');
