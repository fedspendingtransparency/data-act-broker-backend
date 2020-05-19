-- For aggregate and non-aggregate records (RecordType = 1 or 2), with domestic place of performance
-- (PrimaryPlaceOfPerformanceCountryCode = USA): if 9-digit PrimaryPlaceOfPerformanceZIP+4 is not provided,
-- PrimaryPlaceOfPerformanceCongressionalDistrict must be provided.
SELECT
    row_number,
    place_of_performance_congr,
    place_of_performance_zip4a,
    place_of_perform_country_c,
    record_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(place_of_performance_zip4a, '') !~ '^\d\d\d\d\d\-?\d\d\d\d$'
    AND COALESCE(place_of_performance_congr, '') = ''
    AND UPPER(place_of_perform_country_c) = 'USA'
    AND record_type in (1, 2)
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
