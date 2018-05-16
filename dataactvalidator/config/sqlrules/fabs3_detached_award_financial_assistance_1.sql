-- Action type is required for non-aggregate and PII-redacted non-aggregate records (i.e., when RecordType = 2 or 3)
SELECT
    row_number,
    action_type,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type IN (2, 3)
    AND (action_type = ''
        OR action_type IS NULL
    );
