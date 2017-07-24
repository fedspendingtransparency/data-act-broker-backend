SELECT af.row_number,
	af.allocation_transfer_agency,
	af.agency_identifier,
	af.beginning_period_of_availa,
	af.ending_period_of_availabil,
	af.availability_type_code,
	af.main_account_code,
	af.sub_account_code,
	af.program_activity_code,
	af.object_class
FROM award_financial AS af
WHERE af.submission_id = {}
	AND NOT EXISTS (
		SELECT 1
		FROM object_class_program_activity AS op
		WHERE af.tas_id IS NOT DISTINCT FROM op.tas_id
			AND (af.program_activity_code IS NOT DISTINCT FROM op.program_activity_code
				OR COALESCE(af.program_activity_code, '') = ''
				OR af.program_activity_code = '0000')
			AND (af.object_class IS NOT DISTINCT FROM op.object_class
				OR (af.object_class IN ('0', '00', '000', '0000')
					AND af.object_class IN ('0', '00', '000', '0000')
					)
				)
			AND af.submission_id = op.submission_id
	);
