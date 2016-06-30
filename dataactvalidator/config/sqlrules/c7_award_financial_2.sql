SELECT
	row_number,
	gross_outlays_delivered_or_fyb,
	ussgl490800_authority_outl_fyb
FROM award_financial
WHERE submission_id = {}
AND COALESCE(gross_outlays_delivered_or_fyb,0) <>
	COALESCE(ussgl490800_authority_outl_fyb,0);