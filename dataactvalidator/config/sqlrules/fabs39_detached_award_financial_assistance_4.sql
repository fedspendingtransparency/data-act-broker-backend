-- PrimaryPlaceOfPerformanceCode must be blank for record type 3.
SELECT
    row_number,
    record_type,
    place_of_performance_code,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 3
    AND COALESCE(place_of_performance_code, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
