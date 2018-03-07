-- If LegalEntityCongressionalDistrict is provided, it must be valid in the 5-digit zip code indicated by
-- LegalEntityZIP5.
SELECT
    row_number,
    legal_entity_zip5,
    legal_entity_congressional
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(legal_entity_zip5, '') <> ''
    AND COALESCE(legal_entity_congressional, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM zips AS z
        WHERE z.zip5 = dafa.legal_entity_zip5
            AND z.congressional_district_no = dafa.legal_entity_congressional
    );
