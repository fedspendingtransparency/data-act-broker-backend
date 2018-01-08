-- Must be a valid program activity name and code for the corresponding TAS/TAFS as defined in Section 82 of OMB
-- Circular A-11. If the program activity is unknown, enter "0000" and "Unknown/Other" as the code and name,
-- respectively.
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
WHERE af.program_activity_code <> '0000'
    AND UPPER(af.program_activity_name) <> 'UNKNOWN/OTHER'
    AND NOT EXISTS (
        SELECT 1
        FROM program_activity AS pa
        WHERE af.agency_identifier = pa.agency_id
            AND af.main_account_code = pa.account_number
            AND UPPER(COALESCE(af.program_activity_name, '')) = UPPER(pa.program_activity_name)
            AND COALESCE(af.program_activity_code, '') = pa.program_activity_code
            AND CAST(pa.budget_year AS INTEGER) IN (2016, 2017, 2018)  -- temporarily hardcoded to 2016-2018
                                                   -- (SELECT reporting_fiscal_year
                                                   --  FROM submission
                                                   --  WHERE submission_id = {0})
    );
