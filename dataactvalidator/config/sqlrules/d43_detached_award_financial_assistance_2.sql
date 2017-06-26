--  If PrimaryPlaceOfPerformanceCode is not USA, Congressional District must be blank
WITH detached_award_financial_assistance_d43_2_{0} AS
    (SELECT submission_id,
    	row_number,
    	place_of_performance_zip4a,
    	place_of_performance_congr
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    dafa.row_number,
    dafa.place_of_performance_zip4a,
    dafa.place_of_performance_congr
FROM detached_award_financial_assistance_d43_2_{0} AS dafa
WHERE 
	CASE WHEN COALESCE(dafa.place_of_performance_zip4a, '') != ''
        -- 5 digit zip
        THEN (CASE WHEN dafa.place_of_performance_zip4a ~ '^\d\d\d\d\d$'
             THEN NOT EXISTS (SELECT DISTINCT sub_dafa.row_number
                              FROM detached_award_financial_assistance_d43_2_{0} AS sub_dafa
                              JOIN zips
                                ON sub_dafa.place_of_performance_zip4a = zips.zip5
                                    AND sub_dafa.place_of_performance_congr = zips.congressional_district_no)
             -- 9 digit zip
             ELSE (CASE WHEN dafa.place_of_performance_zip4a ~ '^\d\d\d\d\d\d\d\d\d$' OR dafa.place_of_performance_zip4a ~ '^\d\d\d\d\d\-\d\d\d\d$'
                    THEN NOT EXISTS (SELECT DISTINCT sub_dafa.row_number
                                    FROM detached_award_financial_assistance_d43_2_{0} AS sub_dafa
                                    JOIN zips
                                      ON LEFT(sub_dafa.place_of_performance_zip4a, 5) = zips.zip5
                                        AND SUBSTRING(REPLACE(sub_dafa.place_of_performance_zip4a, '-', ''), 6, 4) = zips.zip_last4
                                        AND sub_dafa.place_of_performance_congr = zips.congressional_district_no)
                    -- if any other format, fail
                    ELSE TRUE
                    END)
             END)
        -- if not zip4, just let it pass
        ELSE FALSE
        END