SELECT afa.row_number, afa.fain, afa.uri
FROM award_financial_assistance AS afa
WHERE afa.submission_id = {}
	AND afa.fain IS NOT NULL
	AND afa.uri IS NOT NULL
	AND afa.federal_action_obligation > 0
	AND afa.row_number NOT IN (
		SELECT afa.row_number
		FROM award_financial_assistance AS afa
			JOIN award_financial AS af
				ON afa.fain = af.fain
				AND afa.uri = af.uri
				AND afa.submission_id = af.submission_id
		WHERE afa.federal_action_obligation > 0
	);