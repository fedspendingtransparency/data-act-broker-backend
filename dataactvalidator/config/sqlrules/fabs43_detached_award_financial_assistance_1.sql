-- For foreign places of performance (PrimaryPlaceOfPerformanceCountryCode is not USA),
-- PrimaryPlaceOfPerformanceCongressionalDistrict must be blank.
SELECT
    row_number,
    place_of_perform_country_c,
    place_of_performance_congr,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(place_of_perform_country_c) <> 'USA'
    AND COALESCE(place_of_performance_congr, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
