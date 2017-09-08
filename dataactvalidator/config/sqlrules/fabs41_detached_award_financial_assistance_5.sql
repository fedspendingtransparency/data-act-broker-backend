-- The provided PrimaryPlaceofPerformanceZIP+4 must be in the state specified by PrimaryPlaceOfPerformanceCode.
-- In this specific submission row, the first five digits are valid and located in the correct state,
-- but the last 4 are invalid.
WITH detached_award_financial_assistance_fabs41_5_{0} AS
    (SELECT submission_id,
    	row_number,
    	place_of_performance_code,
    	place_of_performance_zip4a
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    dafa.row_number,
    dafa.place_of_performance_code,
    dafa.place_of_performance_zip4a
FROM detached_award_financial_assistance_fabs41_5_{0} AS dafa
WHERE COALESCE(dafa.place_of_performance_zip4a, '') != ''
    AND dafa.place_of_performance_zip4a != 'city-wide'
    AND dafa.place_of_performance_zip4a ~ '^\d\d\d\d\d\-?\d\d\d\d$'
    AND EXISTS (SELECT *
                FROM zips
                WHERE UPPER(LEFT(dafa.place_of_performance_code, 2)) = zips.state_abbreviation
                    AND LEFT(dafa.place_of_performance_zip4a, 5) = zips.zip5)
    AND NOT EXISTS (SELECT *
                    FROM zips
                    WHERE UPPER(LEFT(dafa.place_of_performance_code, 2)) = zips.state_abbreviation
                        AND LEFT(dafa.place_of_performance_zip4a, 5) = zips.zip5
                        AND RIGHT(dafa.place_of_performance_zip4a, 4) = zips.zip_last4)