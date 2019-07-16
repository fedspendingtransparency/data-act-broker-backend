-- LegalEntityAddressLine2 is optional, but must be blank for aggregate and PII-redacted non-aggregate records
-- (i.e., when RecordType = 1 or 3)
SELECT
    row_number,
    record_type,
    legal_entity_address_line2
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type IN (1, 3)
    AND COALESCE(legal_entity_address_line2, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
