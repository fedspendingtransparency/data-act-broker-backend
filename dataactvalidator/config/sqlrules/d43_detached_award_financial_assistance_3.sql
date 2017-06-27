-- If no PrimaryPlaceOfPerformanceZIP+4 is provided, PrimaryPlaceOfPerformanceCongressionalDistrict must be provided.
-- This congressional district provided must exist in the state indicated by the PrimaryPlaceOfPerformanceCode. The
-- PrimaryPlaceOfPerformanceCongressionalDistrict may be 90 if the state has more than one congressional district.
WITH detached_award_financial_assistance_d43_3_{0} AS
    (SELECT submission_id,
        row_number,
        place_of_performance_code,
        place_of_performance_zip4a,
        place_of_performance_congr
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    dafa.row_number,
    dafa.place_of_performance_code,
    dafa.place_of_performance_zip4a,
    dafa.place_of_performance_congr
FROM detached_award_financial_assistance_d43_3_{0} AS dafa
WHERE COALESCE(dafa.place_of_performance_zip4a, '') = '' 
    AND (COALESCE(dafa.place_of_performance_congr, '') = ''
        OR NOT EXISTS (
            SELECT DISTINCT sub_dafa.row_number
            FROM detached_award_financial_assistance_d43_3_{0} AS sub_dafa
            JOIN zips
              ON UPPER(LEFT(sub_dafa.place_of_performance_code, 2)) = zips.state_abbreviation
                AND sub_dafa.place_of_performance_congr = zips.congressional_district_no)
        OR (dafa.place_of_performance_congr = '90'
            AND dafa.place_of_performance_code != '00*****'
            AND (SELECT COUNT(DISTINCT zips.congressional_district_no)
                FROM zips
                WHERE UPPER(LEFT(dafa.place_of_performance_code, 2)) = zips.state_abbreviation) < 2)
        )