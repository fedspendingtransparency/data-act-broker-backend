-- When PrimaryPlaceOfPerformanceCode is in XX#####, XXTS###, XX####T, or XX####R format,
-- PrimaryPlaceOfPerformanceZIP+4 must not be blank (containing either a zip code or ‘city-wide’).
SELECT
    row_number,
    place_of_performance_code,
    place_of_performance_zip4a,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND (UPPER(fabs.place_of_performance_code) ~ '^[A-Z][A-Z]\d\d\d\d[\dRT]$'
        OR UPPER(fabs.place_of_performance_code) ~ '^[A-Z][A-Z]TS\d\d\d$')
    AND COALESCE(place_of_performance_zip4a, '') = ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
