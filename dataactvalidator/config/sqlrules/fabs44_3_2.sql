-- If LegalEntityCongressionalDistrict is provided, it must be valid in the state or territory indicated by
-- LegalEntityZIP5. The LegalEntityCongressionalDistrict may be 90 if the state has more than one congressional
-- district.
-- Retrieve the state code from the zip5 and then check to make sure the
-- submission's congressional district exists within the state's congressional districts
SELECT
    row_number,
    legal_entity_zip5,
    legal_entity_congressional,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(legal_entity_zip5, '') <> ''
    AND COALESCE(legal_entity_congressional, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
    AND ((legal_entity_congressional <> '90'
            AND NOT EXISTS (
                SELECT 1
                FROM state_congressional AS sc_1
                    INNER JOIN zip_city AS zc_1
                        ON zc_1.state_code = sc_1.state_code
                WHERE sc_1.congressional_district_no = fabs.legal_entity_congressional
                    AND zc_1.zip_code = fabs.legal_entity_zip5
                    AND COALESCE(sc_1.census_year, 2020) >= 2020)
        )
        OR (legal_entity_congressional = '90'
            AND (SELECT COUNT(DISTINCT sc_2.congressional_district_no)
                FROM state_congressional AS sc_2
                    INNER JOIN zip_city AS zc_2
                        ON zc_2.state_code = sc_2.state_code
                WHERE zc_2.zip_code = fabs.legal_entity_zip5
                    AND COALESCE(sc_2.census_year, 2020) >= 2020) < 2)
    )
    AND cast_as_date(action_date) > '01/03/2023';
