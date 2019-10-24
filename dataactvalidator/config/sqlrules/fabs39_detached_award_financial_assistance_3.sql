-- For aggregate or non-aggregate records (RecordType = 1 or 2): PrimaryPlaceofPerformanceZIP+4 must not be provided for
-- any format of PrimaryPlaceOfPerformanceCode other than XX##### or XX####R."
SELECT
    row_number,
    record_type,
    place_of_performance_code,
    place_of_performance_zip4a,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type IN (1, 2)
    AND UPPER(place_of_performance_code) !~ '^[A-Z][A-Z]\d\d\d\d[\dR]$'
    AND COALESCE(place_of_performance_zip4a, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
