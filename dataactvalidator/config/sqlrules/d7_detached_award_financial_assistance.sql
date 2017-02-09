-- URI is a required field for aggregate records (i.e., when RecordType = 1)
SELECT
    row_number,
    record_type,
    uri
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 1
    AND (uri IS NULL or uri = '');