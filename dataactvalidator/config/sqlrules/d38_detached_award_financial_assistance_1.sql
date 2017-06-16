-- 00***** is a valid PrimaryPlaceOfPerformanceCode value and indicates a multi-state project.
-- 00FORGN indicates that the place of performance is in a foreign country (allow it to pass, don't test).
-- If neither of the above, PrimaryPlaceOfPerformanceCode must start with valid 2 character state abbreviation
SELECT
    dafa.row_number,
    dafa.place_of_performance_code
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND dafa.place_of_performance_code != '00*****'
    AND UPPER(dafa.place_of_performance_code) != '00FORGN'
    AND NOT EXISTS (
        SELECT *
        FROM detached_award_financial_assistance AS sub_dafa
        JOIN zips
            ON UPPER(SUBSTRING(dafa.place_of_performance_code, 1, 2)) = zips.state_abbreviation
    )