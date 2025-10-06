-- LegalEntityZIP5 must be blank for aggregate records (i.e., when RecordType = 1)
SELECT
    row_number,
    record_type,
    legal_entity_zip5,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND record_type = 1
    AND COALESCE(legal_entity_zip5, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
