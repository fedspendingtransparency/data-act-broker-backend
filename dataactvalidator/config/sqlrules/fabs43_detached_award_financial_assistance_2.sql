-- If PrimaryPlaceOfPerformanceCongressionalDistrict is not provided, a 9 digit PrimaryPlaceOfPerformanceZIP+4 must be
-- provided
-- Only applies to domestic PPoP (PPoPCountry Code = USA) and
-- for aggregate and non-aggregate records (RecordType = 1 or 2)
SELECT
    row_number,
    place_of_performance_congr,
    place_of_performance_zip4a,
    place_of_perform_country_c,
    record_type,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(place_of_performance_zip4a, '') !~ '^\d\d\d\d\d\-?\d\d\d\d$'
    AND COALESCE(place_of_performance_congr, '') = ''
    AND UPPER(place_of_perform_country_c) = 'USA'
    AND record_type in (1, 2)
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
