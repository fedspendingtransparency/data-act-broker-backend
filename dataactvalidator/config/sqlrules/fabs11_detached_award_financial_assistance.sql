-- LegalEntityAddressLine2 is optional, but must be blank for aggregate records (i.e., when RecordType = 1)
SELECT
    row_number,
    record_type,
    legal_entity_address_line2
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 1
    AND legal_entity_address_line2 != ''
    AND legal_entity_address_line2 IS NOT NULL