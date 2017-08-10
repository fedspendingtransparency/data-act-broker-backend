-- LegalEntityZIPLast4 must be blank for foreign recipients (i.e., when LegalEntityCountryCode is not USA)
SELECT
    row_number,
    legal_entity_country_code,
    legal_entity_zip_last4
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(legal_entity_country_code) != 'USA'
    AND legal_entity_zip_last4 != ''
    AND legal_entity_zip_last4 IS NOT NULL