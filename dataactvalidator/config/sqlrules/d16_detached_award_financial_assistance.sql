-- LegalEntityForeignProvinceName must be blank for foreign recipients (i.e., when LegalEntityCountryCode = USA)
SELECT
    row_number,
    legal_entity_country_code,
    legal_entity_foreign_provi
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(legal_entity_country_code) = 'USA'
    AND legal_entity_foreign_provi IS NOT NULL
    AND legal_entity_foreign_provi != ''
