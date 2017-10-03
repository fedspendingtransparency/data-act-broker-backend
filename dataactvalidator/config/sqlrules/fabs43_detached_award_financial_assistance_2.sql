-- If PrimaryPlaceOfPerformanceCongressionalDistrict is not provided, a 9 digit PrimaryPlaceOfPerformanceZIP+4 must be
-- provided
-- Only applies to domestic PPoP (PPoPCountry Code = USA)
SELECT
    row_number,
    place_of_performance_congr,
    place_of_performance_zip4a,
    place_of_perform_country_c
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(place_of_performance_zip4a, '') !~ '^\d\d\d\d\d\-?\d\d\d\d$'
    AND COALESCE(place_of_performance_congr, '') = ''
    AND UPPER(place_of_perform_country_c) = 'USA'