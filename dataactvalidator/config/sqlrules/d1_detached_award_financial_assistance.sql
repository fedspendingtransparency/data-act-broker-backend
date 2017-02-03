-- fain is required for non-aggregate records (i.e., when RecordType = 2)
SELECT
    row_number,
    record_type,
    fain
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 2
    AND (fain = '' OR fain IS NULL)
