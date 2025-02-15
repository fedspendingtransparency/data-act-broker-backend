-- Must contain REDACTED DUE TO PII for PII-redacted non-aggregate records (i.e., when RecordType = 3).
SELECT
    row_number,
    record_type,
    awardee_or_recipient_legal,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND record_type = 3
    AND UPPER(awardee_or_recipient_legal) <> 'REDACTED DUE TO PII'
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
