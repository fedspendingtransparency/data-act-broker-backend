-- If PrimaryPlaceOfPerformanceCongressionalDistrict is provided it must be valid in the state indicated by
-- PrimaryPlaceOfPerformanceCode. The PrimaryPlaceOfPerformanceCongressionalDistrict may be 90 if the state has more
-- than one congressional district or PrimaryPlaceOfPerformanceCode is 00*****
-- Districts that were created under the 2000 census or later are considered valid for purposes of this rule.
-- This rule is ignored if PrimaryPlaceOfPerformanceCode is blank
WITH fabs43_4_{0} AS
    (SELECT submission_id,
        row_number,
        place_of_performance_code,
        place_of_performance_congr,
        correction_delete_indicatr,
        afa_generated_unique
    FROM fabs
    WHERE submission_id = {0})
SELECT
    fabs.row_number,
    fabs.place_of_performance_code,
    fabs.place_of_performance_congr,
    fabs.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs43_4_{0} AS fabs
WHERE COALESCE(fabs.place_of_performance_code, '') <> ''
    AND CASE WHEN COALESCE(fabs.place_of_performance_congr, '') <> ''
            THEN ((fabs.place_of_performance_congr <> '90'
                    AND fabs.row_number NOT IN (
                        SELECT DISTINCT sub_fabs.row_number
                        FROM fabs43_4_{0} AS sub_fabs
                        JOIN state_congressional AS sc_1
                            ON UPPER(LEFT(sub_fabs.place_of_performance_code, 2)) = sc_1.state_code
                            AND sub_fabs.place_of_performance_congr = sc_1.congressional_district_no
                            AND COALESCE(sc_1.census_year, 2010) >= 2000
                        )
                )
                OR (fabs.place_of_performance_congr = '90'
                    AND fabs.place_of_performance_code <> '00*****'
                    AND (SELECT COUNT(DISTINCT sc_2.congressional_district_no)
                        FROM state_congressional AS sc_2
                        WHERE UPPER(LEFT(fabs.place_of_performance_code, 2)) = sc_2.state_code) < 2

                )
            )
            ELSE FALSE
        END
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
