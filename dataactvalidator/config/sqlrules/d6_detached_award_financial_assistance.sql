-- Record type is required and cannot be blank. It must be 1 or 2.
SELECT
    row_number,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(record_type, -1) NOT IN (1, 2)