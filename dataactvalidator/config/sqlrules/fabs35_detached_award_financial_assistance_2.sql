-- LegalEntityZIP5 is not a valid zip code.
SELECT
    dafa.row_number,
    dafa.legal_entity_zip5
FROM detached_award_financial_assistance AS dafa
WHERE dafa.submission_id = {0}
    AND COALESCE(dafa.legal_entity_zip5, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM zips AS z
        WHERE z.zip5 = dafa.legal_entity_zip5
    );
