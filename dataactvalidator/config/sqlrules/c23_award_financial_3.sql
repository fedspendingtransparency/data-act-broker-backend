SELECT
	NULL as row_number,
	af.fain,
	(SELECT SUM(sub_af.transaction_obligated_amou) AS transaction_sum
		FROM award_financial as sub_af
		WHERE submission_id = {0} AND sub_af.fain = af.fain) AS transaction_obligated_amou_sum,
  (SELECT SUM(sub_afa.federal_action_obligation::numeric) AS obligation_sum
		FROM award_financial_assistance as sub_afa
		WHERE submission_id = {0} AND sub_afa.fain = af.fain and COALESCE(sub_afa.assistance_type,"") in ('07','08')) AS federal_action_obligation_sum,
	(SELECT SUM(sub_afa.original_loan_subsidy_cost::numeric) AS obligation_sum
		FROM award_financial_assistance as sub_afa
		WHERE submission_id = {0} AND sub_afa.fain = af.fain and COALESCE(sub_afa.assistance_type,"") not in ('07','08')) AS original_loan_subsidy_cost_sum

FROM award_financial AS af
JOIN award_financial_assistance AS afa
		ON af.fain = afa.fain
	  AND af.submission_id = afa.submission_id
WHERE af.submission_id = {0}
GROUP BY af.fain
HAVING (SELECT SUM(sub_af.transaction_obligated_amou) AS transaction_sum
		FROM award_financial as sub_af
		WHERE submission_id = {0} AND sub_af.fain = af.fain) <> (-1*(SELECT SUM(sub_afa.federal_action_obligation::numeric) AS obligation_sum
		FROM award_financial_assistance as sub_afa
		WHERE submission_id = {0} AND sub_afa.fain = af.fain and COALESCE(sub_afa.assistance_type,"") in ('07','08')) - (SELECT SUM(sub_afa.original_loan_subsidy_cost::numeric) AS obligation_sum
		FROM award_financial_assistance as sub_afa
		WHERE submission_id = {0} AND sub_afa.fain = af.fain and COALESCE(sub_afa.assistance_type,"") not in ('07','08')))
		