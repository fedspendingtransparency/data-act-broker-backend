SELECT
    row_number,
    fiscal_year_and_quarter_co
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND fiscal_year_and_quarter_co !~ '^\d\d\d\d[1-4]$'