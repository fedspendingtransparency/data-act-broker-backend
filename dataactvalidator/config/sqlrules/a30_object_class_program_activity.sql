SELECT op.row_number,
	op.allocation_transfer_agency,
	op.agency_identifier,
	op.beginning_period_of_availa,
	op.ending_period_of_availabil,
	op.availability_type_code,
	op.main_account_code,
	op.sub_account_code
FROM object_class_program_activity AS op
WHERE op.submission_id = {}
	AND NOT EXISTS (
		SELECT 1
		FROM appropriation AS approp
		WHERE op.tas IS NOT DISTINCT FROM approp.tas
			AND op.submission_id = approp.submission_id
	);