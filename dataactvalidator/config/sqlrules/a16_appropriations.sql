SELECT row_number,
	budget_authority_unobligat_fyb
FROM appropriation
WHERE submission_id = {0}
	AND is_first_quarter = TRUE
	AND budget_authority_unobligat_fyb IS NOT DISTINCT FROM NULL;