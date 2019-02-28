-- Must be a valid program activity name and code for the corresponding TAS/TAFS as defined in Section 82 of OMB
-- Circular A-11. If the program activity is unknown, enter "0000" and "Unknown/Other" as the code and name,
-- respectively. The rule should not trigger at all for re-certifications of FY17Q2 and FY17Q3.
WITH award_financial_b9_{0} AS
    (SELECT submission_id,
        row_number,
        agency_identifier,
        main_account_code,
        program_activity_name,
        program_activity_code
    FROM award_financial
    WHERE submission_id = {0})
SELECT
    af.row_number,
    af.agency_identifier,
    af.main_account_code,
    af.program_activity_name,
    af.program_activity_code
FROM award_financial_b9_{0} AS af
     INNER JOIN submission AS sub
        ON af.submission_id = sub.submission_id
WHERE (af.program_activity_code <> '0000'
        OR UPPER(af.program_activity_name) <> 'UNKNOWN/OTHER')
    AND (sub.reporting_fiscal_year, sub.reporting_fiscal_period) NOT IN (('2017', 6), ('2017', 9))
    AND NOT EXISTS (
        SELECT 1
        FROM program_activity AS pa
        WHERE af.agency_identifier = pa.agency_id
            AND af.main_account_code = pa.account_number
            AND UPPER(COALESCE(af.program_activity_name, '')) = UPPER(pa.program_activity_name)
            AND UPPER(COALESCE(af.program_activity_code, '')) = UPPER(pa.program_activity_code)
            AND pa.fiscal_year_quarter = 'FY' || RIGHT(CAST(sub.reporting_fiscal_year AS CHAR(4)), 2) || 'Q' || sub.reporting_fiscal_period / 3

    );
