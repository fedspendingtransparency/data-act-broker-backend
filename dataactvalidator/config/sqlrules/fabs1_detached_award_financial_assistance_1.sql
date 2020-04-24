-- fain is required for non-aggregate and PII-redacted non-aggregate records (i.e., when RecordType = 2 or 3)
SELECT
    row_number,
    record_type,
    fain,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type IN (2, 3)
    AND (fain = ''
        OR fain IS NULL
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
