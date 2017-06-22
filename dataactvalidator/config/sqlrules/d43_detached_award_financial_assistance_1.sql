--  If PrimaryPlaceOfPerformanceCode is not USA, Congressional District must be blank

SELECT
    row_number,
    place_of_performance_code
FROM detached_award_financial_assistance
WHERE submission_id = {0}
	AND place_of_performance_code = 
    AND COALESCE(place_of_performance_congr) = 0