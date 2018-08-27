-- LegalEntityCongressionalDistrict must be blank for aggregate records (RecordType = 1).

SELECT
    row_number,
    legal_entity_congressional,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(legal_entity_congressional, '') <> ''
    AND record_type = 1;