-- Must be a valid program activity name/program activity code combination for the corresponding funding TAS/TAFS,
-- as defined in the OMB Program Activity MAX Collect Exercise. However, if every balance on this row is $0 there are
-- no obligations or outlays on the TAS, a program activity name of "Unknown/Other" combined with a program activity
-- code of 0000 should be used. Note: A program activity code of "0000" or a program activity name of "Unknown/Other"
-- should not be provided for File C.
WITH award_financial_b9_{0} AS
    (SELECT submission_id,
        row_number,
        agency_identifier,
        main_account_code,
        program_activity_name,
        program_activity_code,
        display_tas
    FROM award_financial
    WHERE submission_id = {0})
SELECT
    af.row_number,
    af.agency_identifier,
    af.main_account_code,
    af.program_activity_name,
    af.program_activity_code,
    af.display_tas AS "uniqueid_TAS",
    af.program_activity_code AS "uniqueid_ProgramActivityCode"
FROM award_financial_b9_{0} AS af
     INNER JOIN submission AS sub
        ON af.submission_id = sub.submission_id
WHERE
    (sub.reporting_fiscal_year, sub.reporting_fiscal_period) NOT IN (('2017', 6), ('2017', 9))
    AND (af.program_activity_code IS NOT NULL OR af.program_activity_name IS NOT NULL)
    AND (
        (af.program_activity_code = '0000' AND UPPER(af.program_activity_name) = 'UNKNOWN/OTHER')
        OR NOT EXISTS (
            SELECT 1
            FROM program_activity AS pa
            WHERE af.agency_identifier = pa.agency_id
                AND af.main_account_code = pa.account_number
                AND UPPER(COALESCE(af.program_activity_name, '')) = UPPER(pa.program_activity_name)
                AND UPPER(COALESCE(af.program_activity_code, '')) = UPPER(pa.program_activity_code)
                AND pa.fiscal_year_period = 'FY' || RIGHT(CAST(sub.reporting_fiscal_year AS CHAR(4)), 2) || 'P' || LPAD(sub.reporting_fiscal_period::text, 2, '0')
        )
    );
