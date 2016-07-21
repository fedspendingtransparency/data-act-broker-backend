SELECT row_number,
	gross_outlay_amount_by_pro_fyb,
	gross_outlays_delivered_or_fyb,
	gross_outlays_undelivered_fyb,
	obligations_delivered_orde_fyb,
	obligations_undelivered_or_fyb,
	ussgl480100_undelivered_or_fyb,
	ussgl480200_undelivered_or_fyb,
	ussgl490100_delivered_orde_fyb,
	ussgl490800_authority_outl_fyb
FROM object_class_program_activity
WHERE submission_id = {0}
	AND is_first_quarter = TRUE
	AND gross_outlay_amount_by_pro_fyb IS NOT DISTINCT FROM NULL
	AND gross_outlays_delivered_or_fyb IS NOT DISTINCT FROM NULL
	AND gross_outlays_undelivered_fyb IS NOT DISTINCT FROM NULL
	AND obligations_delivered_orde_fyb IS NOT DISTINCT FROM NULL
	AND obligations_undelivered_or_fyb IS NOT DISTINCT FROM NULL
	AND ussgl480100_undelivered_or_fyb IS NOT DISTINCT FROM NULL
	AND ussgl480200_undelivered_or_fyb IS NOT DISTINCT FROM NULL
	AND ussgl490100_delivered_orde_fyb IS NOT DISTINCT FROM NULL
	AND ussgl490800_authority_outl_fyb IS NOT DISTINCT FROM NULL;