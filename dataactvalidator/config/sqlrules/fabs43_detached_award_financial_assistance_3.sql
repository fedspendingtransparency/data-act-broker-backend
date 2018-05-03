-- PrimaryPlaceOfPerformanceCongressionalDistrict must be blank for PII-redacted non-aggregate records (RecordType = 3).

SELECT
    row_number,
    place_of_performance_congr,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(place_of_performance_congr) <> ''
    AND record_type = 3;