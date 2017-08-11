-- PrimaryPlaceOfPerformanceForeignLocationDescription is required for foreign places of performance
-- (i.e., when PrimaryPlaceOfPerformanceCountryCode does not equal USA).
SELECT
    row_number,
    place_of_performance_forei,
    place_of_perform_country_c
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(place_of_performance_forei, '') = ''
    AND UPPER(place_of_perform_country_c) != 'USA'