WITH award_financial_c23_{0} AS
    (SELECT piid,
    	transaction_obligated_amou,
    	submission_id
    FROM award_financial
    WHERE submission_id = {0})
SELECT
	NULL as row_number,
	af.piid,
	(SELECT COALESCE(SUM(sub_af.transaction_obligated_amou::numeric),0) AS transaction_sum
		FROM award_financial as sub_af WHERE submission_id = {0} AND sub_af.piid = af.piid) AS transaction_obligated_amou_sum,
  (SELECT COALESCE(SUM(sub_ap.federal_action_obligation), 0) AS obligation_sum
		FROM award_procurement as sub_ap WHERE submission_id = {0} AND sub_ap.piid = af.piid) AS federal_action_obligation_sum
FROM award_financial_c23_{0} AS af
JOIN award_procurement AS ap
		ON af.piid = ap.piid
	  AND af.submission_id = ap.submission_id
GROUP BY af.piid
HAVING
		(SELECT COALESCE(SUM(sub_af.transaction_obligated_amou::numeric),0) AS transaction_sum
			FROM award_financial as sub_af WHERE submission_id = {0} AND sub_af.piid = af.piid) <>
		-1*(SELECT COALESCE(SUM(sub_ap.federal_action_obligation),0) AS obligation_sum
			FROM award_procurement as sub_ap WHERE submission_id = {0} AND sub_ap.piid = af.piid)
		AND NOT EXISTS (SELECT sub_af.allocation_transfer_agency FROM award_financial as sub_af WHERE sub_af.piid = af.piid
			AND COALESCE(sub_af.allocation_transfer_agency,'') <> '')
