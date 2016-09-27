SELECT
	NULL as row_number,
	af.piid,
	SUM(af.transaction_obligated_amou) AS transaction_obligated_amou_sum,
  SUM(afa.federal_action_obligation::numeric) AS federal_action_obligation_sum
FROM award_financial AS af
	JOIN award_financial_assistance AS afa
		ON af.uri = afa.uri
	  AND af.submission_id = afa.submission_id
WHERE af.submission_id = {0}
GROUP BY af.uri
HAVING SUM(af.transaction_obligated_amou) <> SUM(ap.federal_action_obligation::numeric)