-- PrimaryPlaceOfPerformanceCountryCode must contain a valid three character GENC Standard Edition 3.0 (Update 4)
-- country code for record type 1 and 2. U.S. Territories and Freely Associated States must be submitted with country
-- code = USA and their state code. They cannot be submitted with their GENC country code. See Appendix B of the
-- Practices and Procedures.
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
