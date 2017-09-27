-- The provided PrimaryPlaceofPerformanceZIP+4 must be in the state specified by the PrimaryPlaceOfPerformanceCode.
-- In this specific submission row, the ZIP5 (and by extension the full ZIP+4) is not a valid ZIP code
-- in the state in question.
WITH detached_award_financial_assistance_fabs41_3_{0} AS
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
FROM detached_award_financial_assistance_fabs41_3_{0} AS dafa
WHERE CASE WHEN (COALESCE(dafa.place_of_performance_zip4a, '') != ''
                 AND dafa.place_of_performance_zip4a != 'city-wide'
                 AND (dafa.place_of_performance_zip4a ~ '^\d\d\d\d\d$'
                      OR dafa.place_of_performance_zip4a ~ '^\d\d\d\d\d\-?\d\d\d\d$'))
           THEN NOT EXISTS (SELECT *
                            FROM zips
                            WHERE UPPER(LEFT(dafa.place_of_performance_code, 2)) = zips.state_abbreviation
                            AND LEFT(dafa.place_of_performance_zip4a, 5) = zips.zip5)
           ELSE FALSE
           END