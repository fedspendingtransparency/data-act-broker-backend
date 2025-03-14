-- LegalEntityZIPLast4 must be blank for aggregate and PII-redacted non-aggregate records
-- (i.e., when RecordType = 1 or 3)
SELECT
    row_number,
    record_type,
    legal_entity_zip_last4,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND record_type IN (1, 3)
    AND COALESCE(legal_entity_zip_last4, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
