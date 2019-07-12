-- LegalEntityForeignCityName must be blank for domestic recipients when LegalEntityCountryCode is 'USA' and for
-- aggregate records (RecordType = 1).
SELECT
    row_number,
    legal_entity_country_code,
    legal_entity_foreign_city,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (UPPER(legal_entity_country_code) = 'USA'
        OR record_type = 1
    )
    AND COALESCE(legal_entity_foreign_city, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
