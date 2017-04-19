-- For each unique PIID for procurement in File C where ParentAwardId is null, the sum of each
-- TransactionObligatedAmount submitted in the reporting period should match (in inverse) the sum of the
-- FederalActionObligation amounts reported in D1 for the same timeframe, regardless of modifications.
WITH award_financial_c23_1_{0} AS
    (SELECT submission_id,
    	piid,
    	transaction_obligated_amou,
    	parent_award_id,
    	allocation_transfer_agency
    FROM award_financial
    WHERE submission_id = {0}),
award_procurement_c23_1_{0} AS
    (SELECT submission_id,
    	piid,
    	federal_action_obligation
    FROM award_procurement
    WHERE submission_id = {0})
SELECT
	NULL as row_number,
	af.piid,
	(SELECT COALESCE(SUM(sub_af.transaction_obligated_amou::numeric),0) AS transaction_sum
		FROM award_financial_c23_1_{0} as sub_af WHERE sub_af.piid = af.piid AND COALESCE(sub_af.parent_award_id,'') = '') AS transaction_obligated_amou_sum,
    (SELECT COALESCE(SUM(sub_ap.federal_action_obligation), 0) AS obligation_sum
		FROM award_procurement_c23_1_{0} as sub_ap WHERE sub_ap.piid = af.piid) AS federal_action_obligation_sum
FROM award_financial_c23_1_{0} AS af
JOIN award_procurement_c23_1_{0} AS ap
    ON af.piid = ap.piid
    AND (af.parent_award_id IS NULL OR af.parent_award_id = '')
GROUP BY af.piid
HAVING
		(SELECT COALESCE(SUM(sub_af.transaction_obligated_amou::numeric),0) AS transaction_sum
			FROM award_financial_c23_1_{0} as sub_af WHERE sub_af.piid = af.piid AND COALESCE(sub_af.parent_award_id,'') = '') <>
		-1*(SELECT COALESCE(SUM(sub_ap.federal_action_obligation),0) AS obligation_sum
			FROM award_procurement_c23_1_{0} as sub_ap WHERE sub_ap.piid = af.piid)
		AND NOT EXISTS (SELECT sub_af.allocation_transfer_agency FROM award_financial as sub_af WHERE sub_af.piid = af.piid
			AND COALESCE(sub_af.allocation_transfer_agency,'') <> '')