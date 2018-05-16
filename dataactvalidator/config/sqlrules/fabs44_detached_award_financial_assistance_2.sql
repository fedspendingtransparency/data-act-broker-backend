-- For non-aggregate and PII-redacted non-aggregate records (RecordType = 2 or 3)
-- with domestic recipients (LegalEntityCountryCode = USA)
-- If LegalEntityZIPLast4 is not provided and LegalEntityZIP5 is, LegalEntityCongressionalDistrict must be provided.
SELECT
    row_number,
    legal_entity_zip5,
    legal_entity_zip_last4,
    legal_entity_congressional,
    legal_entity_country_code,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(legal_entity_zip5, '') <> ''
    AND COALESCE(legal_entity_zip_last4, '') = ''
    AND COALESCE(legal_entity_congressional, '') = ''
    AND UPPER(legal_entity_country_code) = 'USA'
    AND record_type IN (2,3)
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
