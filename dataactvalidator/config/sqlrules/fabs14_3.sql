-- LegalEntityZIPLast4 is required for domestic recipients (i.e., when LegalEntityCountryCode = USA) for
-- non-aggregate records (i.e., when RecordType = 2)
SELECT
    row_number,
    legal_entity_country_code,
    record_type,
    legal_entity_zip_last4,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND UPPER(legal_entity_country_code) = 'USA'
    AND record_type = 2
    AND COALESCE(legal_entity_zip_last4, '') = ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
