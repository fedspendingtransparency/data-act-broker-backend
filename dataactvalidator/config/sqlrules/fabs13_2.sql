-- LegalEntityZIP5 must be blank for foreign recipients (i.e., when LegalEntityCountryCode is not USA)
SELECT
    row_number,
    legal_entity_country_code,
    legal_entity_zip5,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND UPPER(legal_entity_country_code) <> 'USA'
    AND COALESCE(legal_entity_zip5, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
