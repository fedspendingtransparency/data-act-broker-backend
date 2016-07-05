SELECT afa.row_number, afa.fain, afa.uri
FROM award_financial_assistance AS afa
WHERE afa.submission_id = {0}
	AND afa.federal_action_obligation > 0
	AND afa.row_number NOT IN (
		SELECT afa.row_number
		FROM award_financial_assistance AS afa
			JOIN award_financial AS af
				ON (afa.fain IS NOT DISTINCT FROM af.fain
				AND afa.uri IS NOT DISTINCT FROM af.uri
				AND afa.submission_id = af.submission_id)
		WHERE afa.federal_action_obligation > 0
			AND afa.submission_id = {0}
	);