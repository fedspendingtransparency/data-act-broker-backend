-- If PrimaryPlaceOfPerformanceCongressionalDistrict is provided for an award with a county-wide format
-- PrimaryPlaceOfPerformanceCode with an ActionDate before 20230103, then the
-- PrimaryPlaceOfPerformanceCongressionalDistrict should be associated with the county embedded in the
-- PrimaryPlaceOfPerformanceCode according to the historic USPS source data.
WITH fabs43_5_3_{0} AS
    (SELECT row_number,
        place_of_performance_code,
        place_of_performance_congr,
        correction_delete_indicatr,
        action_date,
        afa_generated_unique
    FROM fabs
    WHERE submission_id = {0}
        AND (CASE WHEN is_date(COALESCE(action_date, '0'))
              THEN CAST(action_date AS DATE)
              END) < CAST('01/03/2023' AS DATE)
        AND UPPER(place_of_performance_code) ~ '^[A-Z][A-Z]\*\*\d\d\d$')
SELECT
    row_number,
    place_of_performance_code,
    place_of_performance_congr,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs43_5_3_{0} AS fabs
WHERE NOT EXISTS (
        SELECT 1
        FROM zips_historical AS zips
        WHERE fabs.place_of_performance_congr = zips.congressional_district_no
            AND UPPER(COALESCE(RIGHT(fabs.place_of_performance_code, 3), '')) = zips.county_number
            AND UPPER(COALESCE(LEFT(fabs.place_of_performance_code, 2), '')) = zips.state_abbreviation
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
