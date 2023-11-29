-- If PrimaryPlaceOfPerformanceCongressionalDistrict is provided for an award with a single-zip format
-- PrimaryPlaceOfPerformanceCode with an ActionDate on or after 20230103, then the
-- PrimaryPlaceOfPerformanceCongressionalDistrict should be associated with the provided PrimaryPlaceOfPerformanceZIP+4
-- according to the current USPS source data.
WITH fabs43_5_2_{0} AS
    (SELECT submission_id,
        row_number,
        place_of_performance_code,
        place_of_performance_congr,
        correction_delete_indicatr,
        place_of_performance_zip4a,
        action_date,
        afa_generated_unique
    FROM fabs
    WHERE submission_id = {0}
        AND (CASE WHEN is_date(COALESCE(action_date, '0'))
              THEN CAST(action_date AS DATE)
              END) >= CAST('01/03/2023' AS DATE)
        AND UPPER(place_of_performance_zip4a) ~ '^\d\d\d\d\d(\-?\d\d\d\d)?$')
SELECT
    row_number,
    place_of_performance_code,
    place_of_performance_congr,
    place_of_performance_zip4a,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs43_5_2_{0} AS fabs
WHERE CASE WHEN place_of_performance_zip4a ~ '^\d\d\d\d\d(\-?\d\d\d\d)?$'
    THEN NOT EXISTS (
        SELECT 1
        FROM zips
        WHERE fabs.place_of_performance_congr = zips.congressional_district_no
            AND UPPER(LEFT(fabs.place_of_performance_zip4a, 5)) = zips.zip5
            AND UPPER(COALESCE(LEFT(fabs.place_of_performance_code, 2), '')) = zips.state_abbreviation
        )
    ELSE NOT EXISTS (
        SELECT 1
        FROM zips
        WHERE fabs.place_of_performance_congr = zips.congressional_district_no
            AND UPPER(LEFT(fabs.place_of_performance_zip4a, 5)) = zips.zip5
            AND UPPER(RIGHT(fabs.place_of_performance_zip4a, 4)) = zips.zip_last4
            AND UPPER(COALESCE(LEFT(fabs.place_of_performance_code, 2), '')) = zips.state_abbreviation
        )
    END
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
