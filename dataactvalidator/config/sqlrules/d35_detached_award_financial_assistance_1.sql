-- LegalEntityZIPLast4 should be provided. No warning when RecordType = 1.
SELECT
    row_number,
    legal_entity_zip_last4,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(legal_entity_zip_last4, '') = ''
    AND record_type != 1