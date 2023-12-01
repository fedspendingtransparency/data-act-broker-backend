-- If PrimaryPlaceOfPerformanceCongressionalDistrict is provided, it must be valid in the state or territory indicated
-- by the PrimaryPlaceOfPerformanceCode. The PrimaryPlaceOfPerformanceCongressionalDistrict may be 90 if the state ha
-- more than one congressional district or PrimaryPlaceOfPerformanceCode is 00*****.
WITH fabs43_4_2_{0} AS
    (SELECT submission_id,
        row_number,
        place_of_performance_code,
        place_of_performance_congr,
        correction_delete_indicatr,
        action_date,
        afa_generated_unique
    FROM fabs
    WHERE submission_id = {0})
SELECT
    row_number,
    place_of_performance_code,
    place_of_performance_congr,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs43_4_2_{0} AS fabs
WHERE COALESCE(place_of_performance_code, '') <> ''
    AND CASE WHEN COALESCE(place_of_performance_congr, '') <> ''
            THEN ((place_of_performance_congr <> '90'
                    AND row_number NOT IN (
                        SELECT DISTINCT sub_fabs.row_number
                        FROM fabs43_4_2_{0} AS sub_fabs
                        JOIN state_congressional AS sc_1
                            ON UPPER(LEFT(sub_fabs.place_of_performance_code, 2)) = sc_1.state_code
                            AND sub_fabs.place_of_performance_congr = sc_1.congressional_district_no
                            AND COALESCE(sc_1.census_year, 2020) >= 2020
                        )
                )
                OR (place_of_performance_congr = '90'
                    AND place_of_performance_code <> '00*****'
                    AND (SELECT COUNT(DISTINCT sc_2.congressional_district_no)
                        FROM state_congressional AS sc_2
                        WHERE UPPER(LEFT(fabs.place_of_performance_code, 2)) = sc_2.state_code
                            AND COALESCE(sc_2.census_year, 2020) >= 2020) < 2
                )
            )
            ELSE FALSE
        END
    AND (CASE WHEN is_date(COALESCE(action_date, '0'))
              THEN CAST(action_date AS DATE)
              END) >= CAST('01/03/2023' AS DATE)
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
