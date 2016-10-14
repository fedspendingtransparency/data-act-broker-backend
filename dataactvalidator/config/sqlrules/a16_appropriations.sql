SELECT row_number,
	budget_authority_unobligat_fyb
FROM appropriation
WHERE submission_id = {0}
	AND EXISTS (SELECT 1 FROM submission WHERE submission_id <> {0} AND cgac_code = (subquery to get cgac))
	AND budget_authority_unobligat_fyb IS NOT DISTINCT FROM NULL;