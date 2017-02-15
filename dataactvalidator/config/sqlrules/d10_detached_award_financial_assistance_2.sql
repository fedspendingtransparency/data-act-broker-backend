-- LegalEntityAddressLine1 must be blank for aggregate records (i.e., when RecordType = 1)
SELECT
    row_number,
    record_type,
    legal_entity_address_line1
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 1
    AND legal_entity_address_line1 != ''
    AND legal_entity_address_line1 IS NOT NULL