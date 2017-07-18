-- LegalEntityZIPLast4 should be provided. No warning when RecordType = 1 or LegalEntityCountryCode != USA.
SELECT
    row_number,
    legal_entity_zip_last4,
    record_type,
    legal_entity_country_code
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(legal_entity_zip_last4, '') = ''
    AND record_type != 1
    AND UPPER(legal_entity_country_code) = 'USA'