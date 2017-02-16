WITH award_financial_c23_2_{0} AS
    (SELECT submission_id,
    	parent_award_id,
    	transaction_obligated_amou
    FROM award_financial
    WHERE submission_id = {0})
SELECT
	NULL as row_number,
	af.parent_award_id,
	(SELECT COALESCE(SUM(sub_af.transaction_obligated_amou::numeric),0) AS transaction_sum
		FROM award_financial as sub_af WHERE submission_id = {0} AND sub_af.parent_award_id = af.parent_award_id) AS transaction_obligated_amou_sum,
  (SELECT COALESCE(SUM(sub_ap.federal_action_obligation),0) AS obligation_sum
		FROM award_procurement as sub_ap WHERE submission_id = {0} AND sub_ap.parent_award_id = af.parent_award_id) AS federal_action_obligation_sum
FROM award_financial_c23_2_{0} AS af
JOIN award_procurement AS ap
		ON af.parent_award_id = ap.parent_award_id
	  AND af.submission_id = ap.submission_id
GROUP BY af.parent_award_id
HAVING 
		(SELECT COALESCE(SUM(sub_af.transaction_obligated_amou::numeric),0) AS transaction_sum
		FROM award_financial as sub_af WHERE submission_id = {0} AND sub_af.parent_award_id = af.parent_award_id) <> 
		-1*(SELECT COALESCE(SUM(sub_ap.federal_action_obligation),0) AS obligation_sum
		FROM award_procurement as sub_ap WHERE submission_id = {0} AND sub_ap.parent_award_id = af.parent_award_id)
		AND NOT EXISTS (SELECT sub_af.allocation_transfer_agency FROM award_financial as sub_af
			WHERE sub_af.parent_award_id = af.parent_award_id AND COALESCE(sub_af.allocation_transfer_agency,'') <> '')
