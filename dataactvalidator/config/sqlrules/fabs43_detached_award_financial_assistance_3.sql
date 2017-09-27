-- If PrimaryPlaceOfPerformanceCongressionalDistrict is provided it must be valid in the state indicated by
-- PrimaryPlaceOfPerformanceCode. The PrimaryPlaceOfPerformanceCongressionalDistrict may be 90 if the state has more
-- than one congressional district or PrimaryPlaceOfPerformanceCode is 00*****
WITH detached_award_financial_assistance_fabs43_3_{0} AS
    (SELECT submission_id,
        row_number,
        place_of_performance_code,
        place_of_performance_congr
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    dafa.row_number,
    dafa.place_of_performance_code,
    dafa.place_of_performance_congr
FROM detached_award_financial_assistance_fabs43_3_{0} AS dafa
WHERE CASE WHEN COALESCE(dafa.place_of_performance_congr, '') != ''
        THEN ((dafa.place_of_performance_congr != '90'
		     AND dafa.row_number NOT IN (
                SELECT DISTINCT sub_dafa.row_number
                FROM detached_award_financial_assistance_fabs43_3_{0} AS sub_dafa
                JOIN state_congressional as sc_1
                  ON UPPER(LEFT(sub_dafa.place_of_performance_code, 2)) = sc_1.state_code
                    AND sub_dafa.place_of_performance_congr = sc_1.congressional_district_no))
             OR (dafa.place_of_performance_congr = '90'
                AND dafa.place_of_performance_code != '00*****'
                AND (SELECT COUNT(DISTINCT sc_2.congressional_district_no)
                    FROM state_congressional as sc_2
                    WHERE UPPER(LEFT(dafa.place_of_performance_code, 2)) = sc_2.state_code) < 2)
            )
        ELSE FALSE
        END