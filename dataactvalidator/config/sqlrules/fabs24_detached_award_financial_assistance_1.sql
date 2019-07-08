-- PrimaryPlaceOfPerformanceCountryCode must be blank for record type 3.
SELECT
    dafa.row_number,
    dafa.record_type,
    dafa.place_of_perform_country_c
FROM detached_award_financial_assistance AS dafa
WHERE submission_id={0}
    AND record_type = 3
    AND COALESCE(place_of_perform_country_c, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
