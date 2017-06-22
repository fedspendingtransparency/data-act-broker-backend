-- PrimaryPlaceOfPerformanceCode for aggregate records (i.e., when RecordType = 1)
-- must be in countywide format (XX**###).
SELECT
    row_number,
    place_of_performance_code,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 1
    AND UPPER(place_of_performance_code) !~ '^[A-Z][A-Z]\*\*\d\d\d$'