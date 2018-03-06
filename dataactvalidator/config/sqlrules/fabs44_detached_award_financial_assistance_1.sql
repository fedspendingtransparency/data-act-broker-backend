-- If LegalEntityZIPLast4 is not provided and LegalEntityZIP5 is, LegalEntityCongressionalDistrict must be provided.
SELECT
    row_number,
    legal_entity_zip5,
    legal_entity_zip_last4,
    legal_entity_congressional
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(legal_entity_zip5, '') <> ''
    AND COALESCE(legal_entity_zip_last4, '') = ''
    AND COALESCE(legal_entity_congressional, '') = '';
