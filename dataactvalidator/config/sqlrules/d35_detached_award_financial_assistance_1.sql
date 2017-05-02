-- LegalEntityZIPLast4 or LegalEntityCongressionalDistrict must be provided.
SELECT
    row_number,
    legal_entity_zip_last4,
    legal_entity_congressional
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(legal_entity_congressional, '') = ''
    AND COALESCE(legal_entity_zip_last4, '') = ''