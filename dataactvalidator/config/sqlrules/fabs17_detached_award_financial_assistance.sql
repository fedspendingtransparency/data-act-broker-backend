-- LegalEntityForeignPostalCode must be blank for domestic recipients when LegalEntityCountryCode is 'USA' and for
-- aggregate records (RecordType = 1).
SELECT
    row_number,
    legal_entity_country_code,
    legal_entity_foreign_posta,
    record_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (UPPER(legal_entity_country_code) = 'USA'
        OR record_type = 1
    )
    AND COALESCE(legal_entity_foreign_posta, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
