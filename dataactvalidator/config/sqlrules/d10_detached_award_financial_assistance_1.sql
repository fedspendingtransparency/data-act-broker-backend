-- LegalEntityAddressLine1 is required for non-aggregate records (i.e., when RecordType = 2)
SELECT
    row_number,
    record_type,
    legal_entity_address_line1
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 2
    AND (legal_entity_address_line1 = '' OR legal_entity_address_line1 IS NULL)