SELECT
	row_number,
	obligations_undelivered_or_fyb,
	ussgl480100_undelivered_or_fyb
FROM object_class_program_activity
WHERE submission_id = {} AND
	COALESCE(obligations_undelivered_or_fyb, 0) <> COALESCE(ussgl480100_undelivered_or_fyb, 0)