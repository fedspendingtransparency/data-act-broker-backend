-- PrimaryPlaceOfPerformanceCode must be in countywide (XX**###), statewide (XX*****) or 00FORGN formats for aggregate
-- records (RecordType = 1).
SELECT
    row_number,
    place_of_performance_code,
    record_type,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 1
    AND NOT (
        UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\*\*\d\d\d$'
        OR UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\*\*\*\*\*$'
        OR UPPER(place_of_performance_code) = '00FORGN'
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
