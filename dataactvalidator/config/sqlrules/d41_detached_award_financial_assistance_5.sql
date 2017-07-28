-- The provided PrimaryPlaceofPerformanceZIP+4 must be in the state specified by PrimaryPlaceOfPerformanceCode. In this specific submission row, the first five digits are valid and located in the correct state, but the last 4 are invalid.
WITH detached_award_financial_assistance_d41_5_{0} AS
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
FROM detached_award_financial_assistance_d41_5_{0} AS dafa
WHERE dafa.place_of_performance_zip4a ~ '^\d\d\d\d\d\-?\d\d\d\d$'
    AND dafa.row_number IN (SELECT DISTINCT sub_dafa.row_number
                            FROM detached_award_financial_assistance_d41_5_{0} AS sub_dafa
                            JOIN zips
                            ON UPPER(LEFT(sub_dafa.place_of_performance_code, 2)) = zips.state_abbreviation
                            AND UPPER(LEFT(sub_dafa.place_of_performance_zip4a, 5)) = zips.zip5)
    AND dafa.row_number NOT IN (SELECT DISTINCT sub_dafa.row_number
                                FROM detached_award_financial_assistance_d41_5_{0} AS sub_dafa
                                JOIN zips
                                ON UPPER(LEFT(sub_dafa.place_of_performance_code, 2)) = zips.state_abbreviation
                                   AND LEFT(sub_dafa.place_of_performance_zip4a, 5) = zips.zip5
                                   AND RIGHT(sub_dafa.place_of_performance_zip4a, 4) = zips.zip_last4)