SELECT row_number,
	gross_outlay_amount_by_awa_fyb,
	gross_outlays_delivered_or_fyb,
	gross_outlays_undelivered_fyb,
	obligations_delivered_orde_fyb,
	obligations_undelivered_or_fyb
FROM award_financial as af
	JOIN submission as sub
	ON sub.submission_id = af.submission_id
WHERE af.submission_id = {0}
	AND NOT EXISTS
		(SELECT 1 FROM submission AS query_sub
		LEFT JOIN publish_status AS ps ON query_sub.publish_status_id = ps.publish_status_id
		WHERE query_sub.submission_id <> {0}
		AND query_sub.cgac_code = sub.cgac_code
		AND query_sub.reporting_fiscal_year = sub.reporting_fiscal_year
		AND (ps.name IN ('published','updated') OR query_sub.publishable = true))
	AND (gross_outlay_amount_by_awa_fyb IS NOT DISTINCT FROM NULL
			OR gross_outlays_delivered_or_fyb IS NOT DISTINCT FROM NULL
			OR gross_outlays_undelivered_fyb IS NOT DISTINCT FROM NULL
			OR obligations_delivered_orde_fyb IS NOT DISTINCT FROM NULL
			OR obligations_undelivered_or_fyb IS NOT DISTINCT FROM NULL)
