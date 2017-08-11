-- LegalEntityZIP5 must be blank for foreign recipients (i.e., when LegalEntityCountryCode is not USA)
SELECT
    row_number,
    legal_entity_country_code,
    legal_entity_zip5
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(legal_entity_country_code) != 'USA'
    AND legal_entity_zip5 != ''
    AND legal_entity_zip5 IS NOT NULL