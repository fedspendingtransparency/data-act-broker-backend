-- If LegalEntityCongressionalDistrict is provided, it must be valid in the state or territory
-- indicated by LegalEntityZIP5. Districts that were created under the 2000 census or later are considered valid.
-- Retrieve the state code from the zip5 and then check to make sure the
-- submission's congressional district exists within the state's congressional districts
SELECT
    row_number,
    legal_entity_zip5,
    legal_entity_congressional,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(legal_entity_zip5, '') <> ''
    AND COALESCE(legal_entity_congressional, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
    AND NOT EXISTS (
        SELECT 1
        FROM state_congressional AS sc
            INNER JOIN zip_city AS zc
                ON zc.state_code = sc.state_code
        WHERE sc.congressional_district_no = fabs.legal_entity_congressional
            AND zc.zip_code = fabs.legal_entity_zip5
            AND COALESCE(sc.census_year, 2010) >= 2000);
