-- PrimaryPlaceOfPerformanceCountryCode must contain a valid three character GENC Standard Edition 3.0 (Update 4)
-- country code.
SELECT
    dafa.row_number,
    dafa.place_of_perform_country_c
FROM detached_award_financial_assistance AS dafa
WHERE submission_id={0}
	AND NOT EXISTS
		(SELECT *
		FROM country_code AS cc
		WHERE UPPER(dafa.place_of_perform_country_c) = UPPER(cc.country_code))