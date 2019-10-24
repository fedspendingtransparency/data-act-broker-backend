-- For foreign recipients (LegalEntityCountryCode is not USA), LegalEntityCongressionalDistrict must be blank.
SELECT
    row_number,
    legal_entity_country_code,
    legal_entity_congressional,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(COALESCE(legal_entity_country_code, '')) <> 'USA'
    AND COALESCE(legal_entity_congressional, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';