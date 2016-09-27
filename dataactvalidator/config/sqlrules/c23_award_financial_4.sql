SELECT
	af.row_number,
	SUM(af.transaction_obligated_amou) AS transaction_obligated_amou_sum,
  SUM(afa.federal_action_obligation) AS federal_action_obligation_sum
FROM award_financial AS af
	JOIN award_financial_assistance AS afa
		ON af.uri = afa.uri
WHERE af.submission_id = {0}
GROUP BY af.uri
HAVING SUM(af.transaction_obligated_amou) <> SUM(ap.federal_action_obligation)