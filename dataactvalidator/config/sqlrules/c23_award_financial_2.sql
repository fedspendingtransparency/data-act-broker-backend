-- For each unique ParentAwardId and PIID for procurement in File C, the sum of each TransactionObligatedAmount
-- submitted in the reporting period should match (in inverse) the sum of the FederalActionObligation amounts reported
-- in D1 for the same timeframe, regardless of modifications.
WITH award_financial_c23_2_{0} AS
    (SELECT piid,
	allocation_transfer_agency,
	transaction_obligated_amou,
	parent_award_id
    FROM award_financial
    WHERE submission_id = {0}),
award_financial_grouped_c23_2_{0} AS
    (SELECT piid,
	parent_award_id,
    	COALESCE(SUM(transaction_obligated_amou), 0) AS sum_ob_amount
    FROM award_financial_c23_2_{0}
    GROUP BY parent_award_id, piid),
award_procurement_c23_2_{0} AS
    (SELECT piid,
	parent_award_id,
    	COALESCE(SUM(federal_action_obligation), 0) AS sum_fed_amount
    FROM award_procurement
    WHERE submission_id = {0}
    GROUP BY parent_award_id, piid)
SELECT
	NULL as row_number,
	af.piid,
	af.parent_award_id,
	af.sum_ob_amount AS transaction_obligated_amou_sum,
	ap.sum_fed_amount AS federal_action_obligation_sum
FROM award_financial_grouped_c23_2_{0} AS af
JOIN award_procurement_c23_2_{0} AS ap
    ON af.parent_award_id = ap.parent_award_id
    AND af.piid = ap.piid
WHERE af.sum_ob_amount <> -1*ap.sum_fed_amount
	AND NOT EXISTS (
		SELECT *
		FROM award_financial_c23_2_{0} AS sub_af
		WHERE sub_af.parent_award_id = af.parent_award_id
			AND sub_af.piid = af.piid
			AND COALESCE(sub_af.allocation_transfer_agency,'') <> '')