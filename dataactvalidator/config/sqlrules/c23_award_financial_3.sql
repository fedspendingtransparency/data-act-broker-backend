SELECT
	NULL as row_number,
	af.fain,
	(SELECT COALESCE(SUM(sub_af.transaction_obligated_amou::numeric),0) AS transaction_sum
		FROM award_financial as sub_af
		WHERE submission_id = {0} AND sub_af.fain = af.fain) AS transaction_obligated_amou_sum,
  (SELECT COALESCE(SUM(sub_afa.federal_action_obligation::numeric),0) AS obligation_sum
		FROM award_financial_assistance as sub_afa
		WHERE submission_id = {0} AND sub_afa.fain = af.fain and
		COALESCE(sub_afa.assistance_type,'') not in ('07','08')) AS federal_action_obligation_sum,
	(SELECT COALESCE(SUM(sub_afa.original_loan_subsidy_cost::numeric),0) AS obligation_sum
		FROM award_financial_assistance as sub_afa
		WHERE submission_id = {0} AND sub_afa.fain = af.fain and
		COALESCE(sub_afa.assistance_type,'') in ('07','08')) AS original_loan_subsidy_cost_sum
FROM award_financial AS af
JOIN award_financial_assistance AS afa
		ON af.fain = afa.fain
	  AND af.submission_id = afa.submission_id
WHERE af.submission_id = {0}
GROUP BY af.fain
HAVING
		(SELECT COALESCE(SUM(sub_af.transaction_obligated_amou::numeric),0) AS transaction_sum
		FROM award_financial as sub_af WHERE submission_id = {0} AND sub_af.fain = af.fain) <>
		(-1*(SELECT COALESCE(SUM(sub_afa.federal_action_obligation::numeric),0) AS obligation_sum
		FROM award_financial_assistance as sub_afa
		WHERE submission_id = {0} AND sub_afa.fain = af.fain and COALESCE(sub_afa.assistance_type,'') not in ('07','08')) -
		(SELECT COALESCE(SUM(sub_afa.original_loan_subsidy_cost::numeric),0) AS obligation_sum
		FROM award_financial_assistance as sub_afa
		WHERE submission_id = {0} AND sub_afa.fain = af.fain and COALESCE(sub_afa.assistance_type,'') in ('07','08')))
		AND NOT EXISTS (SELECT sub_af.allocation_transfer_agency FROM award_financial as sub_af WHERE sub_af.fain = af.fain
			AND COALESCE(sub_af.allocation_transfer_agency,'') <> '')