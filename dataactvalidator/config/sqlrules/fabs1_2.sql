-- FAIN must not be used for aggregate records (RecordType = 1).
SELECT
    row_number,
    record_type,
    fain,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 1
    AND COALESCE(fain, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
