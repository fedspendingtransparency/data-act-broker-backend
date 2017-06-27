WITH detached_award_financial_assistance_d43_2_{0} AS
    (SELECT submission_id,
        row_number,
        place_of_performance_zip4a,
        place_of_performance_congr,
     	place_of_performance_code
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    dafa.row_number,
    dafa.place_of_performance_code,
    dafa.place_of_performance_congr,
    dafa.place_of_performance_zip4a
FROM detached_award_financial_assistance_d43_2_{0} AS dafa
WHERE COALESCE(dafa.place_of_performance_zip4a, '') != ''
     AND CASE WHEN COALESCE(dafa.place_of_performance_congr, '') != ''
        THEN NOT EXISTS (SELECT DISTINCT sub_dafa.row_number
            FROM detached_award_financial_assistance_d43_2_{0} AS sub_dafa
            JOIN zips
                ON UPPER(LEFT(sub_dafa.place_of_performance_code,2)) = zips.state_abbreviation
                 AND sub_dafa.place_of_performance_congr = zips.congressional_district_no)
        ELSE FALSE
        END