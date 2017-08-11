-- If PrimaryPlaceOfPerformanceCongressionalDistrict is provided it must be valid in the state indicated by
-- PrimaryPlaceOfPerformanceCode. The PrimaryPlaceOfPerformanceCongressionalDistrict may be 90 if the state has more
-- than one congressional district or PrimaryPlaceOfPerformanceCode is 00*****
WITH detached_award_financial_assistance_d43_3_{0} AS
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
FROM detached_award_financial_assistance_d43_3_{0} AS dafa
WHERE CASE WHEN COALESCE(dafa.place_of_performance_congr, '') != ''
        THEN ((dafa.place_of_performance_congr != '90'
		     AND dafa.row_number NOT IN (
                SELECT DISTINCT sub_dafa.row_number
                FROM detached_award_financial_assistance_d43_3_{0} AS sub_dafa
                JOIN zips
                  ON UPPER(LEFT(sub_dafa.place_of_performance_code, 2)) = zips.state_abbreviation
                    AND sub_dafa.place_of_performance_congr = zips.congressional_district_no))
             OR (dafa.place_of_performance_congr = '90'
                AND dafa.place_of_performance_code != '00*****'
                AND (SELECT COUNT(DISTINCT zips.congressional_district_no)
                    FROM zips
                    WHERE UPPER(LEFT(dafa.place_of_performance_code, 2)) = zips.state_abbreviation) < 2)
            )
        ELSE FALSE
        END