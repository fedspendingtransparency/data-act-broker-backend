SELECT count(op.row_number),
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
FROM object_class_program_activity as op
WHERE op.submission_id = {0}
GROUP BY op.beginning_period_of_availa,
	op.ending_period_of_availabil,
	op.agency_identifier,
	op.allocation_transfer_agency,
	op.availability_type_code,
	op.main_account_code,
	op.sub_account_code,
	op.object_class,
	op.program_activity_code,
	op.by_direct_reimbursable_fun
HAVING count(op.row_number) > 1;