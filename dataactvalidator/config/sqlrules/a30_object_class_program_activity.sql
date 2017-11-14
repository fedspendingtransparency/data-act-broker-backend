-- All TAS values in File B (object class program activity) should exist in File A (appropriations)
SELECT op.row_number,
	op.allocation_transfer_agency,
	op.agency_identifier,
	op.beginning_period_of_availa,
	op.ending_period_of_availabil,
	op.availability_type_code,
	op.main_account_code,
	op.sub_account_code
FROM object_class_program_activity AS op
WHERE op.submission_id = {0}
	AND NOT EXISTS (
		SELECT 1
		FROM appropriation AS approp
		WHERE op.tas_id IS NOT DISTINCT FROM approp.tas_id
			AND op.submission_id = approp.submission_id
	);
