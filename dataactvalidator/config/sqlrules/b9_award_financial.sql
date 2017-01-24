SELECT af.row_number,
	af.beginning_period_of_availa,
	af.agency_identifier,
	af.allocation_transfer_agency,
	af.main_account_code,
	af.program_activity_name,
	af.program_activity_code
FROM award_financial as af
WHERE af.submission_id = {0}
	AND CAST(COALESCE(af.beginning_period_of_availa,'0') AS integer) >= 2016
	AND af.row_number NOT IN (
		SELECT af.row_number
		FROM award_financial as af
			JOIN program_activity as pa
				ON (af.beginning_period_of_availa = pa.budget_year
				AND af.agency_identifier = pa.agency_id
				AND af.allocation_transfer_agency = pa.allocation_transfer_id
				AND af.main_account_code = pa.account_number
				AND af.program_activity_name = pa.program_activity_name
				AND af.program_activity_code = pa.program_activity_code)
				OR (af.program_activity_name IS NULL AND af.program_activity_code IS NULL)
		WHERE af.submission_id = {0}
	);