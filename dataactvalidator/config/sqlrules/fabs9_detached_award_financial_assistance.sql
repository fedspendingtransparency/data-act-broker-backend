-- Must contain MULTIPLE RECIPIENTS for aggregate records (i.e., when RecordType = 1).
SELECT
    row_number,
    record_type,
    awardee_or_recipient_legal
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 1
    AND LOWER(awardee_or_recipient_legal) <> 'multiple recipients'
