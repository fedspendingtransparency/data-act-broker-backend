-- If LegalEntityCongressionalDistrict is provided, it must be valid in the state or territory
-- indicated by LegalEntityZIP5. Districts that were created under the 2000 census or later are considered valid.
-- Retrieve the state code from the zip5 and congressional district and then check to make sure the
-- submission's congressional district exists within the state's congressional districts
SELECT
    row_number,
    legal_entity_zip5,
    legal_entity_congressional
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(legal_entity_zip5, '') <> ''
    AND COALESCE(legal_entity_congressional, '') <> ''
    AND dafa.legal_entity_congressional
        NOT IN (SELECT sc.congressional_district_no
    	        FROM state_congressional AS sc,
    	 	        zips as z
    	        WHERE z.congressional_district_no = dafa.legal_entity_congressional
    	 	        AND z.zip5 = dafa.legal_entity_zip5
    	            AND z.state_abbreviation = sc.state_code
    	            AND sc.census_year IS NULL OR sc.census_year >= 2000);
