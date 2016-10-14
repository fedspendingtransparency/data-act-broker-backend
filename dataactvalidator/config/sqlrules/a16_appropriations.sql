SELECT row_number,
	budget_authority_unobligat_fyb
FROM appropriation as approp
	JOIN submission as sub
	ON sub.submission_id = approp.submission_id
WHERE submission_id = {0}
	AND NOT EXISTS (SELECT 1 FROM submission AS sub
		JOIN publish_status AS ps ON sub.publish_status_id = ps.publish_status_id
		WHERE submission_id <> {0}
		AND cgac_code = sub.cgac_code
		AND reporting_fiscal_year = sub.reporting_fiscal_year
		AND (ps.name IN ('published','updated') OR sub.publishable = true))
	AND budget_authority_unobligat_fyb IS NOT DISTINCT FROM NULL;