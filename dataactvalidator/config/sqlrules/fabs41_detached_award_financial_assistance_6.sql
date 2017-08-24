-- If PrimaryPlaceOfPerformanceCode is XX00000, PrimaryPlaceOfPerformanceZip4 must not be 'city-wide'
SELECT
    row_number,
    place_of_performance_code,
    place_of_performance_zip4a
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]00000$'
    AND place_of_performance_zip4a = 'city-wide'