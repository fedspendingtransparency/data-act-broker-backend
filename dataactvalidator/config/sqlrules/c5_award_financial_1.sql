SELECT
	row_number,
	gross_outlay_amount_by_awa_cpe,
	gross_outlays_undelivered_cpe,
	gross_outlays_delivered_or_cpe
FROM award_financial
WHERE submission_id = {}
AND COALESCE(gross_outlay_amount_by_awa_cpe,0) <>
	COALESCE(gross_outlays_undelivered_cpe,0) +
	COALESCE(gross_outlays_delivered_or_cpe,0);