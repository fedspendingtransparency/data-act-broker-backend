SELECT af.row_number, af.fain, af.uri
FROM award_financial AS af
WHERE af.submission_id = {}
	AND af.fain IS NOT NULL
	AND af.uri IS NOT NULL
	AND af.row_number NOT IN (
		SELECT af.row_number
		FROM award_financial AS af
			JOIN award_financial_assistance AS afa
				ON af.fain = afa.fain
				AND af.uri = afa.uri
				AND af.submission_id = afa.submission_id
	);