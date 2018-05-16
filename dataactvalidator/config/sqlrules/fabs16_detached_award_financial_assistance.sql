-- LegalEntityForeignProvinceName must be blank for foreign recipients (i.e., when LegalEntityCountryCode = USA) and for
-- aggregate records (RecordType = 1).
SELECT
    row_number,
    legal_entity_country_code,
    legal_entity_foreign_provi,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (UPPER(legal_entity_country_code) = 'USA'
        OR record_type = 1
    )
    AND COALESCE(legal_entity_foreign_provi, '') <> '';
