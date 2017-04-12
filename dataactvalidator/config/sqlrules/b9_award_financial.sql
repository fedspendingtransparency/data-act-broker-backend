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
WHERE af.submission_id = {0}
	AND af.program_activity_code <> '0000'
	AND LOWER(af.program_activity_name) <> 'unknown/other'
	AND af.row_number NOT IN (
		SELECT af.row_number
		FROM award_financial_b9_{0} as af
			JOIN program_activity as pa
                ON (af.agency_identifier IS NOT DISTINCT FROM pa.agency_id
                AND af.main_account_code IS NOT DISTINCT FROM pa.account_number
                AND LOWER(af.program_activity_name) IS NOT DISTINCT FROM pa.program_activity_name
                AND af.program_activity_code IS NOT DISTINCT FROM pa.program_activity_code
                AND CAST(pa.budget_year as integer) = (SELECT reporting_fiscal_year
                                                            FROM submission
                                                            WHERE submission_id = af.submission_id))
	);