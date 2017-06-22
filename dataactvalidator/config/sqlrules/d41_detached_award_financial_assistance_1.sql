-- For PrimaryPlaceOfPerformanceCode XX##### city must exist in provided state (zip4 not provided, error)
SELECT
    dafa.row_number,
    dafa.place_of_performance_code
FROM detached_award_financial_assistance AS dafa
WHERE dafa.submission_id = {0}
    AND UPPER(dafa.place_of_performance_code) ~ '^[A-Z][A-Z]\d\d\d\d\d$'
    AND COALESCE(dafa.place_of_performance_zip4a, '') = ''
    AND NOT EXISTS (
        SELECT *
        FROM detached_award_financial_assistance AS sub_dafa
        JOIN city_code
            ON SUBSTRING(dafa.place_of_performance_code, 3, 5) = city_code.city_code
                AND UPPER(SUBSTRING(dafa.place_of_performance_code, 1, 2)) = city_code.state_code
    )