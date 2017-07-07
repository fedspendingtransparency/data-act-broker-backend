-- When provided, PrimaryPlaceofPerformanceZIP+4 must be in the state specified by PrimaryPlaceOfPerformanceCode.
WITH detached_award_financial_assistance_d41_3_{0} AS
    (SELECT submission_id,
    	row_number,
    	place_of_performance_code,
    	place_of_performance_zip4a
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    dafa.row_number,
    dafa.place_of_performance_code,
    dafa.place_of_performance_zip4a
FROM detached_award_financial_assistance_d41_3_{0} AS dafa
WHERE CASE WHEN COALESCE(dafa.place_of_performance_zip4a, '') != ''
        -- 5 digit zip
        THEN (CASE WHEN dafa.place_of_performance_zip4a ~ '^\d\d\d\d\d$'
             THEN dafa.row_number NOT IN (SELECT DISTINCT sub_dafa.row_number
                              FROM detached_award_financial_assistance_d41_3_{0} AS sub_dafa
                              JOIN zips
                                ON UPPER(LEFT(sub_dafa.place_of_performance_code, 2)) = zips.state_abbreviation
                                    AND sub_dafa.place_of_performance_zip4a = zips.zip5)
             -- 9 digit zip
             ELSE (CASE WHEN dafa.place_of_performance_zip4a ~ '^\d\d\d\d\d\d\d\d\d$' OR dafa.place_of_performance_zip4a ~ '^\d\d\d\d\d\-\d\d\d\d$'
                    THEN dafa.row_number NOT IN (SELECT DISTINCT sub_dafa.row_number
                                    FROM detached_award_financial_assistance_d41_3_{0} AS sub_dafa
                                    JOIN zips
                                      ON UPPER(LEFT(sub_dafa.place_of_performance_code, 2)) = zips.state_abbreviation
                                        AND LEFT(sub_dafa.place_of_performance_zip4a, 5) = zips.zip5
                                        AND SUBSTRING(REPLACE(sub_dafa.place_of_performance_zip4a, '-', ''), 6, 4) = zips.zip_last4)
                    -- if any other format, pass (will check format in another rule)
                    ELSE FALSE
                    END)
             END)
        -- if not zip4, just let it pass
        ELSE FALSE
        END