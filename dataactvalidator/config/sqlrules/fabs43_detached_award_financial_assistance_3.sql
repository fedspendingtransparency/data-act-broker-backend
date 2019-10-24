-- PrimaryPlaceOfPerformanceCongressionalDistrict must be blank for PII-redacted non-aggregate records (RecordType = 3).

SELECT
    row_number,
    place_of_performance_congr,
    record_type,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(place_of_performance_congr, '') <> ''
    AND record_type = 3
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
