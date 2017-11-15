-- For each unique PIID for procurement in File C (award financial) where ParentAwardId is null, the sum of each
-- TransactionObligatedAmount submitted in the reporting period should match (in inverse) the sum of the
-- FederalActionObligation amounts reported in D1 (award procurement) for the same timeframe, regardless of
-- modifications.
WITH award_financial_c23_1_{0} AS
    (SELECT piid,
	allocation_transfer_agency,
	transaction_obligated_amou,
	parent_award_id
    FROM award_financial
    WHERE submission_id = {0}),
-- gather the grouped sum from the previous WITH (we need both so we can do the NOT EXISTS later)
award_financial_grouped_c23_1_{0} AS
    (SELECT piid,
    	COALESCE(SUM(transaction_obligated_amou), 0) AS sum_ob_amount
    FROM award_financial_c23_1_{0}
    WHERE COALESCE(parent_award_id, '') = ''
    GROUP BY piid),
-- gather the grouped sum for award procurement data
award_procurement_c23_1_{0} AS
    (SELECT piid,
    	COALESCE(SUM(federal_action_obligation), 0) AS sum_fed_amount
    FROM award_procurement
    WHERE submission_id = {0}
    GROUP BY piid)
SELECT
	NULL AS row_number,
	af.piid,
	af.sum_ob_amount AS transaction_obligated_amou_sum,
	ap.sum_fed_amount AS federal_action_obligation_sum
FROM award_financial_grouped_c23_1_{0} AS af
JOIN award_procurement_c23_1_{0} AS ap
    ON af.piid = ap.piid
WHERE af.sum_ob_amount <> -1 * ap.sum_fed_amount
	AND NOT EXISTS (
		SELECT 1
		FROM award_financial_c23_1_{0} AS sub_af
		WHERE sub_af.piid = af.piid
			AND COALESCE(sub_af.allocation_transfer_agency,'') <> ''
	);
