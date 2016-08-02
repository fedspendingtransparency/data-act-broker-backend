SELECT row_number,
	beginning_period_of_availa,
	ending_period_of_availabil,
	agency_identifier,
	allocation_transfer_agency,
	availability_type_code,
	main_account_code,
	sub_account_code,
	object_class,
	program_activity_code,
	by_direct_reimbursable_fun
FROM (
	SELECT op.row_number,
		op.beginning_period_of_availa,
		op.ending_period_of_availabil,
		op.agency_identifier,
		op.allocation_transfer_agency,
		op.availability_type_code,
		op.main_account_code,
		op.sub_account_code,
		op.object_class,
		op.program_activity_code,
		op.by_direct_reimbursable_fun,
		op.submission_id,
		ROW_NUMBER() OVER (PARTITION BY
			op.beginning_period_of_availa,
			op.ending_period_of_availabil,
			op.agency_identifier,
			op.allocation_transfer_agency,
			op.availability_type_code,
			op.main_account_code,
			op.sub_account_code,
			op.object_class,
			op.program_activity_code,
			op.by_direct_reimbursable_fun
		) AS row
	FROM object_class_program_activity as op
	WHERE op.submission_id = {0}
	ORDER BY op.row_number
	) duplicates
WHERE duplicates.row > 1;