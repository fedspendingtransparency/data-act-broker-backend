-- PrimaryPlaceOfPerformanceForeignLocationDescription must be blank for domestic recipients
-- (i.e., when PrimaryPlaceOfPerformanceCountryCode = USA) or for for aggregate and PII-redacted non-aggregate
-- records (RecordType=1 or 3).
SELECT
    row_number,
    place_of_performance_forei,
    place_of_perform_country_c,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(place_of_performance_forei, '') <> ''
    AND (UPPER(place_of_perform_country_c) = 'USA'
    OR record_type in (1, 3));
