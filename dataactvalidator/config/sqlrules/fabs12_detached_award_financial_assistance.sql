-- LegalEntityAddressLine3 is optional, but must be blank for aggregate records (i.e., when RecordType = 1)
SELECT
    row_number,
    record_type,
    legal_entity_address_line3
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 1
    AND legal_entity_address_line3 != ''
    AND legal_entity_address_line3 IS NOT NULL