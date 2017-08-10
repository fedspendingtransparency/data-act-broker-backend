-- If PrimaryPlaceOfPerformanceCode is XX00000, PrimaryPlaceOfPerformanceZip4 must not be 'city-wide'
WITH detached_award_financial_assistance_d41_2_{0} AS
    (SELECT submission_id,
    	row_number,
    	place_of_performance_code,
    	place_of_performance_zip4a
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    dafa.row_number,
    dafa.place_of_performance_code
FROM detached_award_financial_assistance_d41_2_{0} AS dafa
WHERE UPPER(dafa.place_of_performance_code) ~ '^[A-Z][A-Z]00000$'
    AND dafa.place_of_performance_zip4a = 'city-wide'