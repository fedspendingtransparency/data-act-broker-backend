SELECT row_number,
	budget_authority_unobligat_fyb
FROM appropriation as approp
	JOIN submission as sub
	ON sub.submission_id = approp.submission_id
WHERE approp.submission_id = {0}
	AND NOT EXISTS
		(SELECT 1 FROM submission AS query_sub
		LEFT JOIN publish_status AS ps ON query_sub.publish_status_id = ps.publish_status_id
		WHERE query_sub.submission_id <> {0}
		AND query_sub.cgac_code = sub.cgac_code
		AND query_sub.reporting_fiscal_year = sub.reporting_fiscal_year
		AND (ps.name IN ('published','updated') OR query_sub.publishable = true))
	AND budget_authority_unobligat_fyb IS NOT DISTINCT FROM NULL;