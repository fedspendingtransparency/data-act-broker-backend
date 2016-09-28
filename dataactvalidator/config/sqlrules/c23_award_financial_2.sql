SELECT
	NULL as row_number,
	af.parent_award_id,
	(SELECT SUM(sub_af.transaction_obligated_amou) AS transaction_sum
		FROM award_financial as sub_af
		WHERE submission_id = {0} AND sub_af.parent_award_id = af.parent_award_id) AS transaction_obligated_amou_sum,
  (SELECT SUM(sub_ap.federal_action_obligation::numeric) AS obligation_sum
		FROM award_procurement as sub_ap
		WHERE submission_id = {0} AND sub_ap.parent_award_id = af.parent_award_id) AS federal_action_obligation_sum
FROM award_financial AS af
JOIN award_procurement AS ap
		ON af.parent_award_id = ap.parent_award_id
	  AND af.submission_id = ap.submission_id
WHERE af.submission_id = {0}
GROUP BY af.parent_award_id
HAVING (SELECT SUM(sub_af.transaction_obligated_amou) AS transaction_sum
		FROM award_financial as sub_af
		WHERE submission_id = {0} AND sub_af.parent_award_id = af.parent_award_id) <> -1*(SELECT SUM(sub_ap.federal_action_obligation::numeric) AS obligation_sum
		FROM award_procurement as sub_ap
		WHERE submission_id = {0} AND sub_ap.parent_award_id = af.parent_award_id)