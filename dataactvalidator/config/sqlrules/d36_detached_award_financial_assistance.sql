-- CFDA_Number must be in XX.XXX or XXX.XXXX format where # represents a number from 0 to 9.
SELECT
    row_number,
    cfda_number
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND dafa.cfda_number !~ '^\d\d\.\d\d\d$'