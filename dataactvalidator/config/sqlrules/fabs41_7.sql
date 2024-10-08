-- PrimaryPlaceOfPerformanceZip+4 must be blank for PII-redacted non-aggregate records (i.e., RecordType = 3).
SELECT
    row_number,
    record_type,
    place_of_performance_zip4a,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND record_type = 3
    AND COALESCE(place_of_performance_zip4a, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
