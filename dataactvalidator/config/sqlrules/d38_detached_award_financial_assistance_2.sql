-- XX**### PrimaryPlaceOfPerformanceCode validates for countywide projects, where ### represents the 3-digit county.
SELECT
    dafa.row_number,
    dafa.place_of_performance_code
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND SUBSTRING(dafa.place_of_performance_code, 3, 2) = '**'
    AND SUBSTRING(dafa.place_of_performance_code, 5, 3) != '***'
    AND NOT EXISTS (
        SELECT *
        FROM detached_award_financial_assistance AS sub_dafa
        JOIN zips
            ON SUBSTRING(dafa.place_of_performance_code, 5, 3) = zips.county_number
    )