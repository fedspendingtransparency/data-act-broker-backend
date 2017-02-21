WITH object_class_program_activity_a16_{0} AS 
	(SELECT submission_id,
		row_number,
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
	WHERE submission_id = {0})
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
FROM object_class_program_activity_a16_{0} as ocpa
	JOIN submission as sub
	ON sub.submission_id = ocpa.submission_id
WHERE NOT EXISTS
		(SELECT 1 FROM submission AS query_sub
		LEFT JOIN publish_status AS ps ON query_sub.publish_status_id = ps.publish_status_id
		WHERE query_sub.submission_id <> {0}
		AND query_sub.cgac_code = sub.cgac_code
		AND query_sub.reporting_fiscal_year = sub.reporting_fiscal_year
		AND (ps.name IN ('published','updated') OR query_sub.publishable = true))
	AND (gross_outlay_amount_by_pro_fyb IS NOT DISTINCT FROM NULL
			OR gross_outlays_delivered_or_fyb IS NOT DISTINCT FROM NULL
			OR gross_outlays_undelivered_fyb IS NOT DISTINCT FROM NULL
			OR obligations_delivered_orde_fyb IS NOT DISTINCT FROM NULL
			OR obligations_undelivered_or_fyb IS NOT DISTINCT FROM NULL
			OR ussgl480100_undelivered_or_fyb IS NOT DISTINCT FROM NULL
			OR ussgl480200_undelivered_or_fyb IS NOT DISTINCT FROM NULL
			OR ussgl490100_delivered_orde_fyb IS NOT DISTINCT FROM NULL
			OR ussgl490800_authority_outl_fyb IS NOT DISTINCT FROM NULL)