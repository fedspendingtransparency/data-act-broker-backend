-- For record type 1 or 2, 00***** is a valid PrimaryPlaceOfPerformanceCode value and indicates a multi-state project.
-- 00FORGN indicates that the place of performance is in a foreign country (allow it to pass, don't test).
-- If neither of the above, PrimaryPlaceOfPerformanceCode must start with valid 2 character state abbreviation
WITH detached_award_financial_assistance_fabs39_1_{0} AS
    (SELECT submission_id,
        row_number,
        record_type,
        place_of_performance_code
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    dafa.row_number,
    dafa.record_type,
    dafa.place_of_performance_code
FROM detached_award_financial_assistance_fabs39_1_{0} AS dafa
WHERE dafa.record_type IN (1, 2)
    AND (COALESCE(dafa.place_of_performance_code, '') = ''
        OR (dafa.place_of_performance_code <> '00*****'
            AND UPPER(dafa.place_of_performance_code) <> '00FORGN'
            AND dafa.row_number NOT IN (
                SELECT DISTINCT sub_dafa.row_number
                FROM detached_award_financial_assistance_fabs39_1_{0} AS sub_dafa
                JOIN states
                    ON UPPER(SUBSTRING(sub_dafa.place_of_performance_code, 1, 2)) = states.state_code
            )
        )
    );
