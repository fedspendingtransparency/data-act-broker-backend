-- LegalEntityCongressionalDistrict must be blank for aggregate records (RecordType = 1).

SELECT
    row_number,
    legal_entity_congressional,
    record_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(legal_entity_congressional, '') <> ''
    AND record_type = 1
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';