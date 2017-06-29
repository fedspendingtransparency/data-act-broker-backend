-- PrimaryPlaceofPerformanceZIP+4 should not be provided for any format of PrimaryPlaceOfPerformanceCode
-- other than XX#####.
SELECT
    row_number,
    place_of_performance_code,
    place_of_performance_zip4a
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(place_of_performance_code) !~ '^[A-Z][A-Z]\d\d\d\d\d$'
    AND COALESCE(place_of_performance_zip4a, '') != ''