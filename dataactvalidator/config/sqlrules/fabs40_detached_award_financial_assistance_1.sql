-- XX**### PrimaryPlaceOfPerformanceCode validates for countywide projects, where ### represents the 3-digit county.
WITH detached_award_financial_assistance_d40_1_{0} AS
    (SELECT submission_id,
    	row_number,
    	place_of_performance_code
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    dafa.row_number,
    dafa.place_of_performance_code
FROM detached_award_financial_assistance_d40_1_{0} AS dafa
WHERE SUBSTRING(dafa.place_of_performance_code, 3, 2) = '**'
    AND SUBSTRING(dafa.place_of_performance_code, 5, 3) != '***'
    AND dafa.row_number NOT IN (
        SELECT DISTINCT sub_dafa.row_number
        FROM detached_award_financial_assistance_d40_1_{0} AS sub_dafa
        JOIN county_code
            ON SUBSTRING(sub_dafa.place_of_performance_code, 5, 3) = county_code.county_number
                AND UPPER(SUBSTRING(sub_dafa.place_of_performance_code, 1, 2)) = county_code.state_code
    )