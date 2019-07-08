-- LegalEntityZIP5 is required for domestic recipients (i.e., when LegalEntityCountryCode = USA)
-- for non-aggregate and PII-redacted non-aggregate records (i.e., when RecordType = 2 or 3)
SELECT
    row_number,
    legal_entity_country_code,
    record_type,
    legal_entity_zip5
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(legal_entity_country_code) = 'USA'
    AND record_type IN (2, 3)
    AND COALESCE(legal_entity_zip5, '') = ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
