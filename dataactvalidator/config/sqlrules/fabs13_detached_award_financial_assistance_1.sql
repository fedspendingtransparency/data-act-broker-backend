-- LegalEntityZIP5 must be blank for aggregate records (i.e., when RecordType = 1)
SELECT
    row_number,
    record_type,
    legal_entity_zip5,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 1
    AND legal_entity_zip5 <> ''
    AND legal_entity_zip5 IS NOT NULL
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
