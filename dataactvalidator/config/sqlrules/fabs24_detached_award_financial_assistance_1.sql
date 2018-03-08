-- PrimaryPlaceOfPerformanceCountryCode must contain a valid three character GENC Standard Edition 3.0 (Update 4)
-- country code for record type 1 and 2.
SELECT
    dafa.row_number,
    dafa.record_type,
    dafa.place_of_perform_country_c
FROM detached_award_financial_assistance AS dafa
WHERE submission_id={0}
    AND record_type IN (1, 2)
    AND NOT EXISTS (
        SELECT 1
        FROM country_code AS cc
        WHERE UPPER(dafa.place_of_perform_country_c) = UPPER(cc.country_code)
    );
