-- The provided PrimaryPlaceofPerformanceZIP+4 must be in the state specified by the PrimaryPlaceOfPerformanceCode.
-- In this specific submission row, the ZIP5 (and by extension the full ZIP+4) is not a valid ZIP code
-- in the state in question.
WITH fabs41_3_{0} AS
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
FROM fabs41_3_{0} AS fabs
WHERE CASE WHEN (COALESCE(fabs.place_of_performance_zip4a, '') <> ''
                 AND fabs.place_of_performance_zip4a <> 'city-wide'
                 AND fabs.place_of_performance_zip4a ~ '^\d\d\d\d\d(\-?\d\d\d\d)?$'
           )
           THEN NOT EXISTS (SELECT *
                            FROM zips
                            WHERE UPPER(LEFT(fabs.place_of_performance_code, 2)) = zips.state_abbreviation
                                AND LEFT(fabs.place_of_performance_zip4a, 5) = zips.zip5)
           ELSE FALSE
       END
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
