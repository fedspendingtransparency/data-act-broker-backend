-- When PrimaryPlaceOfPerformanceCode is in XX##### or XX####R format, PrimaryPlaceOfPerformanceZIP+4 must not be
-- blank (containing either a zip code or ‘city-wide’).
SELECT
    row_number,
    place_of_performance_code,
    place_of_performance_zip4a,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\d\d\d\d[\dR]$'
    AND COALESCE(place_of_performance_zip4a, '') = ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
