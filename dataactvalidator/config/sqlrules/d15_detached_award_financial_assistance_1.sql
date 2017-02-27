-- LegalEntityForeignCityName is required for foreign recipients (i.e., when LegalEntityCountryCode != USA)

SELECT
    row_number,
    legal_entity_country_code,
    legal_entity_foreign_city
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(legal_entity_country_code) != 'USA'
    AND (legal_entity_foreign_city IS NULL OR legal_entity_foreign_city = '')

