-- XX**### PrimaryPlaceOfPerformanceCode validates for countywide projects, where ### represents the 3-digit county.
WITH fabs40_1_{0} AS
    (SELECT submission_id,
        row_number,
        place_of_performance_code,
        correction_delete_indicatr,
        afa_generated_unique
    FROM fabs
    WHERE submission_id = {0})
SELECT
    fabs.row_number,
    fabs.place_of_performance_code,
    fabs.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs40_1_{0} AS fabs
WHERE SUBSTRING(fabs.place_of_performance_code, 3, 2) = '**'
    AND SUBSTRING(fabs.place_of_performance_code, 5, 3) <> '***'
    AND fabs.row_number NOT IN (
        SELECT DISTINCT sub_fabs.row_number
        FROM fabs40_1_{0} AS sub_fabs
        JOIN county_code
            ON SUBSTRING(sub_fabs.place_of_performance_code, 5, 3) = county_code.county_number
                AND UPPER(SUBSTRING(sub_fabs.place_of_performance_code, 1, 2)) = county_code.state_code
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
