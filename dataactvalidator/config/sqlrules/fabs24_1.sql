-- PrimaryPlaceOfPerformanceCountryCode must be blank for record type 3.
SELECT
    row_number,
    record_type,
    place_of_perform_country_c,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id={0}
    AND record_type = 3
    AND COALESCE(place_of_perform_country_c, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
