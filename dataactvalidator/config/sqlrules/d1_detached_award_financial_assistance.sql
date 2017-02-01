SELECT
    row_number,
    record_type,
    fain
FROM detached_award_financial_assistance as dafa
WHERE dafa.submission_id = {0}
AND dafa.record_type = 2
AND dafa.fain IS NULL
