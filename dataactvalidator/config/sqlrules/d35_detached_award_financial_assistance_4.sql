-- When provided, LegalEntityZIPLast4 must be in the format ####.
SELECT
    row_number,
    legal_entity_zip_last4
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(legal_entity_zip_last4, '') != ''
    AND legal_entity_zip_last4 !~ '^\d\d\d\d$'