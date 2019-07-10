-- For aggregate or non-aggregate records (RecordType = 1 or 2): PrimaryPlaceOfPerformanceCode must be 00FORGN for
-- foreign places of performance (PrimaryPlaceOfPerformanceCountryCode is not USA) and must not be 00FORGN otherwise.
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
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
