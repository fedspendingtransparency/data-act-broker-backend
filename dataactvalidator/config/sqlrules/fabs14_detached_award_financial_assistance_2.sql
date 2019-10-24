-- LegalEntityZIPLast4 must be blank for foreign recipients (i.e., when LegalEntityCountryCode is not USA)
SELECT
    row_number,
    legal_entity_country_code,
    legal_entity_zip_last4,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(legal_entity_country_code) <> 'USA'
    AND legal_entity_zip_last4 <> ''
    AND legal_entity_zip_last4 IS NOT NULL
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
