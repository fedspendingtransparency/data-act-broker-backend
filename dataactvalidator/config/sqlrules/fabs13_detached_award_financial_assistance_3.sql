-- LegalEntityZIP5 is required for domestic recipients (i.e., when LegalEntityCountryCode = USA)
-- for non-aggregate records (i.e., when RecordType = 2)
SELECT
    row_number,
    legal_entity_country_code,
    record_type,
    legal_entity_zip5
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(legal_entity_country_code) = 'USA'
    AND record_type = 2
    AND (legal_entity_zip5 = '' OR legal_entity_zip5 IS NULL)