-- For each unique URI for financial assistance in File C, the sum of each TransactionObligatedAmount submitted in the
-- reporting period should match (in inverse) the sum of the FederalActionObligation and OriginalLoanSubsidyCost
-- amounts reported in D2 for the same timeframe, regardless of modifications.
WITH award_financial_c23_4_{0} AS
    (SELECT submission_id,
    	transaction_obligated_amou,
    	uri,
    	allocation_transfer_agency
    FROM award_financial
    WHERE submission_id = {0}),
award_financial_assistance_c23_4_{0} AS
    (SELECT submission_id,
    	federal_action_obligation,
    	uri,
    	original_loan_subsidy_cost,
    	assistance_type
    FROM award_financial_assistance
    WHERE submission_id = {0})
SELECT
	NULL as row_number,
	af.uri,
	(SELECT COALESCE(SUM(sub_af.transaction_obligated_amou::numeric),0) AS transaction_sum
		FROM award_financial_c23_4_{0} as sub_af
		WHERE sub_af.uri = af.uri) AS transaction_obligated_amou_sum,
  (SELECT COALESCE(SUM(sub_afa.federal_action_obligation),0) AS obligation_sum
		FROM award_financial_assistance_c23_4_{0} as sub_afa
		WHERE sub_afa.uri = af.uri and
		COALESCE(sub_afa.assistance_type,'') not in ('07','08')) AS federal_action_obligation_sum,
	(SELECT COALESCE(SUM(sub_afa.original_loan_subsidy_cost::numeric),0) AS obligation_sum
		FROM award_financial_assistance_c23_4_{0} as sub_afa
		WHERE sub_afa.uri = af.uri and
		COALESCE(sub_afa.assistance_type,'') in ('07','08')) AS original_loan_subsidy_cost_sum
FROM award_financial_c23_4_{0} AS af
JOIN award_financial_assistance_c23_4_{0} AS afa
    ON af.uri = afa.uri
GROUP BY af.uri
HAVING
		(SELECT COALESCE(SUM(sub_af.transaction_obligated_amou::numeric),0) AS transaction_sum
		FROM award_financial_c23_4_{0}  as sub_af WHERE sub_af.uri = af.uri) <>
		(-1*(SELECT COALESCE(SUM(sub_afa.federal_action_obligation),0) AS obligation_sum
		FROM award_financial_assistance_c23_4_{0} as sub_afa
		WHERE sub_afa.uri = af.uri and COALESCE(sub_afa.assistance_type,'') not in ('07','08')) -
		(SELECT COALESCE(SUM(sub_afa.original_loan_subsidy_cost::numeric),0) AS obligation_sum
		FROM award_financial_assistance_c23_4_{0} as sub_afa
		WHERE sub_afa.uri = af.uri and COALESCE(sub_afa.assistance_type,'') in ('07','08')))
		AND NOT EXISTS (SELECT sub_af.allocation_transfer_agency FROM award_financial as sub_af WHERE sub_af.uri = af.uri
			AND COALESCE(sub_af.allocation_transfer_agency,'') <> '')
