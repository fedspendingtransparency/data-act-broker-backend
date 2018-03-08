-- PrimaryPlaceOfPerformanceCode must be 00FORGN when PrimaryPlaceofPerformanceCountryCode is not USA,
-- not 00FORGN otherwise for record type 1 and 2.
SELECT
    row_number,
    record_type,
    place_of_performance_code,
    place_of_perform_country_c
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type IN (1, 2)
    AND ((UPPER(place_of_perform_country_c) = 'USA'
            AND UPPER(place_of_performance_code) = '00FORGN'
        )
        OR (UPPER(place_of_perform_country_c) <> 'USA'
            AND UPPER(place_of_performance_code) <> '00FORGN'
        )
    )