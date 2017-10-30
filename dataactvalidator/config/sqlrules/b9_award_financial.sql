WITH award_financial_b9_{0} AS
	(SELECT submission_id,
		row_number,
		agency_identifier,
		main_account_code,
		program_activity_name,
		program_activity_code
	FROM award_financial
	WHERE submission_id = {0})
SELECT af.row_number,
	af.agency_identifier,
	af.main_account_code,
	af.program_activity_name,
	af.program_activity_code
FROM award_financial_b9_{0} as af
WHERE af.program_activity_code <> '0000'
	AND LOWER(af.program_activity_name) <> 'unknown/other'
	AND NOT EXISTS (
		SELECT *
		FROM program_activity AS pa
                WHERE (af.agency_identifier = pa.agency_id
                AND af.main_account_code = pa.account_number
                AND LOWER(af.program_activity_name) IS NOT DISTINCT FROM pa.program_activity_name
                AND af.program_activity_code IS NOT DISTINCT FROM pa.program_activity_code
                AND (CAST(pa.budget_year as integer) in (2016, (SELECT reporting_fiscal_year
                                                                    FROM submission
                                                                    WHERE submission_id = {0}))))
	);
