-- PrimaryPlaceOfPerformanceCode must be 00FORGN when PrimaryPlaceofPerformanceCountryCode is not USA,
-- not 00FORGN otherwise.
SELECT
    row_number,
    place_of_performance_code,
    place_of_perform_country_c
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (
        (UPPER(place_of_perform_country_c) = 'USA' AND UPPER(place_of_performance_code) = '00FORGN')
        OR
        (UPPER(place_of_perform_country_c) != 'USA' AND UPPER(place_of_performance_code) != '00FORGN')
    )