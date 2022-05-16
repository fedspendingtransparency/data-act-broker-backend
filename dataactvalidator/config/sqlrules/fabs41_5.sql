-- The provided PrimaryPlaceofPerformanceZIP+4 must be in the state specified by PrimaryPlaceOfPerformanceCode.
-- In this specific submission row, the first five digits are valid and located in the correct state,
-- but the last 4 are invalid.
WITH fabs41_5_{0} AS
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
    fabs.place_of_performance_zip4a,
    fabs.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs41_5_{0} AS fabs
WHERE COALESCE(fabs.place_of_performance_zip4a, '') <> ''
    AND fabs.place_of_performance_zip4a <> 'city-wide'
    AND fabs.place_of_performance_zip4a ~ '^\d\d\d\d\d\-?\d\d\d\d$'
    AND EXISTS (
        SELECT *
        FROM zips
        WHERE UPPER(LEFT(fabs.place_of_performance_code, 2)) = zips.state_abbreviation
            AND LEFT(fabs.place_of_performance_zip4a, 5) = zips.zip5
    )
    AND NOT EXISTS (
        SELECT *
        FROM zips
        WHERE UPPER(LEFT(fabs.place_of_performance_code, 2)) = zips.state_abbreviation
            AND LEFT(fabs.place_of_performance_zip4a, 5) = zips.zip5
            AND RIGHT(fabs.place_of_performance_zip4a, 4) = zips.zip_last4
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
