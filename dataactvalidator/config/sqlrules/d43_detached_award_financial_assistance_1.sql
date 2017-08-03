--  If PrimaryPlaceOfPerformanceCountryCode is not USA, Congressional District must be blank
SELECT
    row_number,
    place_of_perform_country_c,
    place_of_performance_congr
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(place_of_perform_country_c) != 'USA'
    AND COALESCE(place_of_performance_congr,'') != ''