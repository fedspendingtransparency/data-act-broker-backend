-- LegalEntityAddressLine1 is required for non-aggregate records (i.e., when RecordType = 2)
SELECT
    row_number,
    record_type,
    legal_entity_address_line1,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND record_type = 2
    AND COALESCE(legal_entity_address_line1, '') = ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
