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
FROM object_class_program_activity as ocpa
	JOIN submission as sub
	ON sub.submission_id = ocpa.submission_id
WHERE submission_id = {0}
	AND NOT EXISTS (SELECT 1 FROM submission AS sub
		JOIN publish_status AS ps ON sub.publish_status_id = ps.publish_status_id
		WHERE submission_id <> {0}
		AND cgac_code = sub.cgac_code
		AND reporting_fiscal_year = sub.reporting_fiscal_year
		AND (ps.name IN ('published','updated') OR sub.publishable = true))
	AND gross_outlay_amount_by_pro_fyb IS NOT DISTINCT FROM NULL
	AND gross_outlays_delivered_or_fyb IS NOT DISTINCT FROM NULL
	AND gross_outlays_undelivered_fyb IS NOT DISTINCT FROM NULL
	AND obligations_delivered_orde_fyb IS NOT DISTINCT FROM NULL
	AND obligations_undelivered_or_fyb IS NOT DISTINCT FROM NULL
	AND ussgl480100_undelivered_or_fyb IS NOT DISTINCT FROM NULL
	AND ussgl480200_undelivered_or_fyb IS NOT DISTINCT FROM NULL
	AND ussgl490100_delivered_orde_fyb IS NOT DISTINCT FROM NULL
	AND ussgl490800_authority_outl_fyb IS NOT DISTINCT FROM NULL;