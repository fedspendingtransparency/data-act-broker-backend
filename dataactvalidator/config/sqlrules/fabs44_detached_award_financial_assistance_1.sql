-- For foreign recipients (LegalEntityCountryCode is not USA), LegalEntityCongressionalDistrict must be blank.
SELECT
    row_number,
    legal_entity_country_code,
    legal_entity_congressional
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(COALESCE(legal_entity_country_code, '')) <> 'USA'
    AND COALESCE(legal_entity_congressional, '') <> '';