-- For PrimaryPlaceOfPerformanceCode XX##### or XX####R, where PrimaryPlaceOfPerformanceZIP+4 is "city-wide":
-- city code ##### or ####R must be valid and exist in the provided state.
WITH fabs41_1_{0} AS
    (SELECT submission_id,
        row_number,
        place_of_performance_code,
        place_of_performance_zip4a,
        correction_delete_indicatr,
        afa_generated_unique
    FROM fabs
    WHERE submission_id = {0})
SELECT
    fabs.row_number,
    fabs.place_of_performance_code,
    fabs.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs41_1_{0} AS fabs
WHERE UPPER(fabs.place_of_performance_code) ~ '^[A-Z][A-Z]\d\d\d\d[\dR]$'
    AND (COALESCE(fabs.place_of_performance_zip4a, '') = ''
         OR fabs.place_of_performance_zip4a = 'city-wide'
    )
    AND fabs.row_number NOT IN (
        SELECT DISTINCT sub_fabs.row_number
        FROM fabs41_1_{0} AS sub_fabs
        JOIN city_code
            ON SUBSTRING(sub_fabs.place_of_performance_code, 3, 5) = city_code.city_code
                AND UPPER(SUBSTRING(sub_fabs.place_of_performance_code, 1, 2)) = city_code.state_code
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';