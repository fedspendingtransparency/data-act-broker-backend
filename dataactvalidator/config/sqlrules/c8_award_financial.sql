SELECT af.row_number, af.fain, af.uri
FROM award_financial AS af
WHERE af.submission_id = {0}
	AND af.row_number NOT IN (
		SELECT af.row_number
		FROM award_financial AS af
			JOIN award_financial_assistance AS afa
				ON (af.fain IS NOT DISTINCT FROM afa.fain
				AND af.uri IS NOT DISTINCT FROM afa.uri
				AND af.submission_id = afa.submission_id)
		WHERE af.submission_id = {0}
	);