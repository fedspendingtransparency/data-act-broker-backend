SELECT op.row_number,
	op.beginning_period_of_availa,
	op.agency_identifier,
	op.allocation_transfer_agency,
	op.main_account_code,
	op.program_activity_name,
	op.program_activity_code
FROM object_class_program_activity as op
WHERE op.submission_id = {0}
    AND CAST(COALESCE(op.beginning_period_of_availa,'0') AS integer) IN (SELECT DISTINCT CAST(budget_year AS integer) FROM program_activity)
	AND op.row_number NOT IN (
		SELECT op.row_number
		FROM object_class_program_activity as op
			JOIN program_activity as pa
				ON (op.beginning_period_of_availa = pa.budget_year
				AND op.agency_identifier = pa.agency_id
				AND op.allocation_transfer_agency = pa.allocation_transfer_id
				AND op.main_account_code = pa.account_number
				AND op.program_activity_name = pa.program_activity_name
				AND op.program_activity_code = pa.program_activity_code)
		WHERE op.submission_id = {0}
	);