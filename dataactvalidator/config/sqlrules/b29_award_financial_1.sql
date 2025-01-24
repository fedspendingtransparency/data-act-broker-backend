-- For File C PriorYearAdjustment must be one of X, B, P, or Blank
SELECT
    row_number,
    prior_year_adjustment,
    display_tas AS "uniqueid_TAS"
FROM award_financial
WHERE submission_id = {0}
    AND UPPER(COALESCE(prior_year_adjustment, '')) NOT IN ('X', 'B', 'P', '');
