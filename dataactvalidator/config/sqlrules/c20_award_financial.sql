WITH af_sum (ussgl480100_undelivered_or_fyb_sum,
    ussgl480100_undelivered_or_cpe_sum,
    ussgl483100_undelivered_or_cpe_sum,
    ussgl488100_upward_adjustm_cpe_sum,
    obligations_undelivered_or_fyb_sum,
    obligations_undelivered_or_cpe_sum,
    ussgl490100_delivered_orde_fyb_sum,
    ussgl490100_delivered_orde_cpe_sum,
    ussgl493100_delivered_orde_cpe_sum,
    ussgl498100_upward_adjustm_cpe_sum,
    obligations_delivered_orde_fyb_sum,
    obligations_delivered_orde_cpe_sum,
    ussgl480200_undelivered_or_fyb_sum,
    ussgl480200_undelivered_or_cpe_sum,
    ussgl483200_undelivered_or_cpe_sum,
    ussgl488200_upward_adjustm_cpe_sum,
    gross_outlays_undelivered_fyb_sum,
    gross_outlays_undelivered_cpe_sum,
    ussgl490200_delivered_orde_cpe_sum,
    ussgl490800_authority_outl_fyb_sum,
    ussgl490800_authority_outl_cpe_sum,
    ussgl498200_upward_adjustm_cpe_sum,
    gross_outlays_delivered_or_fyb_sum,
    gross_outlays_delivered_or_cpe_sum,
    gross_outlay_amount_by_awa_fyb_sum,
    gross_outlay_amount_by_awa_cpe_sum,
    obligations_incurred_byawa_cpe_sum,
    ussgl487100_downward_adjus_cpe_sum,
    ussgl497100_downward_adjus_cpe_sum,
    ussgl487200_downward_adjus_cpe_sum,
    ussgl497200_downward_adjus_cpe_sum,
    deobligations_recov_by_awa_cpe_sum,
    tas,
    object_class,
    submission_id) AS
	(SELECT SUM(ussgl480100_undelivered_or_fyb) AS ussgl480100_undelivered_or_fyb_sum,
		SUM(ussgl480100_undelivered_or_cpe) AS ussgl480100_undelivered_or_cpe_sum,
		SUM(ussgl483100_undelivered_or_cpe) AS ussgl483100_undelivered_or_cpe_sum,
		SUM(ussgl488100_upward_adjustm_cpe) AS ussgl488100_upward_adjustm_cpe_sum,
		SUM(obligations_undelivered_or_fyb) AS obligations_undelivered_or_fyb_sum,
		SUM(obligations_undelivered_or_cpe) AS obligations_undelivered_or_cpe_sum,
		SUM(ussgl490100_delivered_orde_fyb) AS ussgl490100_delivered_orde_fyb_sum,
		SUM(ussgl490100_delivered_orde_cpe) AS ussgl490100_delivered_orde_cpe_sum,
		SUM(ussgl493100_delivered_orde_cpe) AS ussgl493100_delivered_orde_cpe_sum,
		SUM(ussgl498100_upward_adjustm_cpe) AS ussgl498100_upward_adjustm_cpe_sum,
		SUM(obligations_delivered_orde_fyb) AS obligations_delivered_orde_fyb_sum,
		SUM(obligations_delivered_orde_cpe) AS obligations_delivered_orde_cpe_sum,
		SUM(ussgl480200_undelivered_or_fyb) AS ussgl480200_undelivered_or_fyb_sum,
		SUM(ussgl480200_undelivered_or_cpe) AS ussgl480200_undelivered_or_cpe_sum,
		SUM(ussgl483200_undelivered_or_cpe) AS ussgl483200_undelivered_or_cpe_sum,
		SUM(ussgl488200_upward_adjustm_cpe) AS ussgl488200_upward_adjustm_cpe_sum,
		SUM(gross_outlays_undelivered_fyb) AS gross_outlays_undelivered_fyb_sum,
		SUM(gross_outlays_undelivered_cpe) AS gross_outlays_undelivered_cpe_sum,
		SUM(ussgl490200_delivered_orde_cpe) AS ussgl490200_delivered_orde_cpe_sum,
		SUM(ussgl490800_authority_outl_fyb) AS ussgl490800_authority_outl_fyb_sum,
		SUM(ussgl490800_authority_outl_cpe) AS ussgl490800_authority_outl_cpe_sum,
		SUM(ussgl498200_upward_adjustm_cpe) AS ussgl498200_upward_adjustm_cpe_sum,
		SUM(gross_outlays_delivered_or_fyb) AS gross_outlays_delivered_or_fyb_sum,
		SUM(gross_outlays_delivered_or_cpe) AS gross_outlays_delivered_or_cpe_sum,
		SUM(gross_outlay_amount_by_awa_fyb) AS gross_outlay_amount_by_awa_fyb_sum,
		SUM(gross_outlay_amount_by_awa_cpe) AS gross_outlay_amount_by_awa_cpe_sum,
		SUM(obligations_incurred_byawa_cpe) AS obligations_incurred_byawa_cpe_sum,
		SUM(ussgl487100_downward_adjus_cpe) AS ussgl487100_downward_adjus_cpe_sum,
		SUM(ussgl497100_downward_adjus_cpe) AS ussgl497100_downward_adjus_cpe_sum,
		SUM(ussgl487200_downward_adjus_cpe) AS ussgl487200_downward_adjus_cpe_sum,
		SUM(ussgl497200_downward_adjus_cpe) AS ussgl497200_downward_adjus_cpe_sum,
		SUM(deobligations_recov_by_awa_cpe) AS deobligations_recov_by_awa_cpe_sum,
		tas,
		object_class,
		submission_id
	FROM award_financial
	WHERE submission_id = {0}
	GROUP BY tas, object_class, submission_id
	)
SELECT
	op.row_number
FROM af_sum
	JOIN object_class_program_activity AS op
		ON af_sum.tas IS NOT DISTINCT FROM op.tas
		AND af_sum.object_class IS NOT DISTINCT FROM op.object_class
		AND af_sum.submission_id = op.submission_id
GROUP BY op.row_number,
	ussgl480100_undelivered_or_fyb_sum,
    ussgl480100_undelivered_or_cpe_sum,
    ussgl483100_undelivered_or_cpe_sum,
    ussgl488100_upward_adjustm_cpe_sum,
    obligations_undelivered_or_fyb_sum,
    obligations_undelivered_or_cpe_sum,
    ussgl490100_delivered_orde_fyb_sum,
    ussgl490100_delivered_orde_cpe_sum,
    ussgl493100_delivered_orde_cpe_sum,
    ussgl498100_upward_adjustm_cpe_sum,
    obligations_delivered_orde_fyb_sum,
    obligations_delivered_orde_cpe_sum,
    ussgl480200_undelivered_or_fyb_sum,
    ussgl480200_undelivered_or_cpe_sum,
    ussgl483200_undelivered_or_cpe_sum,
    ussgl488200_upward_adjustm_cpe_sum,
    gross_outlays_undelivered_fyb_sum,
    gross_outlays_undelivered_cpe_sum,
    ussgl490200_delivered_orde_cpe_sum,
    ussgl490800_authority_outl_fyb_sum,
    ussgl490800_authority_outl_cpe_sum,
    ussgl498200_upward_adjustm_cpe_sum,
    gross_outlays_delivered_or_fyb_sum,
    gross_outlays_delivered_or_cpe_sum,
    gross_outlay_amount_by_awa_fyb_sum,
    gross_outlay_amount_by_awa_cpe_sum,
    obligations_incurred_byawa_cpe_sum,
    ussgl487100_downward_adjus_cpe_sum,
    ussgl497100_downward_adjus_cpe_sum,
    ussgl487200_downward_adjus_cpe_sum,
    ussgl497200_downward_adjus_cpe_sum,
    deobligations_recov_by_awa_cpe_sum,
    op.ussgl480100_undelivered_or_fyb,
    op.ussgl480100_undelivered_or_cpe,
    op.ussgl483100_undelivered_or_cpe,
    op.ussgl488100_upward_adjustm_cpe,
    op.obligations_undelivered_or_fyb,
    op.obligations_undelivered_or_cpe,
    op.ussgl490100_delivered_orde_fyb,
    op.ussgl490100_delivered_orde_cpe,
    op.ussgl493100_delivered_orde_cpe,
    op.ussgl498100_upward_adjustm_cpe,
    op.obligations_delivered_orde_fyb,
    op.obligations_delivered_orde_cpe,
    op.ussgl480200_undelivered_or_fyb,
    op.ussgl480200_undelivered_or_cpe,
    op.ussgl483200_undelivered_or_cpe,
    op.ussgl488200_upward_adjustm_cpe,
    op.gross_outlays_undelivered_fyb,
    op.gross_outlays_undelivered_cpe,
    op.ussgl490200_delivered_orde_cpe,
    op.ussgl490800_authority_outl_fyb,
    op.ussgl490800_authority_outl_cpe,
    op.ussgl498200_upward_adjustm_cpe,
    op.gross_outlays_delivered_or_fyb,
    op.gross_outlays_delivered_or_cpe,
    op.gross_outlay_amount_by_pro_fyb,
    op.gross_outlay_amount_by_pro_cpe,
    op.obligations_incurred_by_pr_cpe,
    op.ussgl487100_downward_adjus_cpe,
    op.ussgl497100_downward_adjus_cpe,
    op.ussgl487200_downward_adjus_cpe,
    op.ussgl497200_downward_adjus_cpe,
    op.deobligations_recov_by_pro_cpe
HAVING ussgl480100_undelivered_or_fyb_sum < op.ussgl480100_undelivered_or_fyb
    AND ussgl480100_undelivered_or_cpe_sum < op.ussgl480100_undelivered_or_cpe
    AND ussgl483100_undelivered_or_cpe_sum < op.ussgl483100_undelivered_or_cpe
    AND ussgl488100_upward_adjustm_cpe_sum < op.ussgl488100_upward_adjustm_cpe
    AND obligations_undelivered_or_fyb_sum < op.obligations_undelivered_or_fyb
    AND obligations_undelivered_or_cpe_sum < op.obligations_undelivered_or_cpe
    AND ussgl490100_delivered_orde_fyb_sum < op.ussgl490100_delivered_orde_fyb
    AND ussgl490100_delivered_orde_cpe_sum < op.ussgl490100_delivered_orde_cpe
    AND ussgl493100_delivered_orde_cpe_sum < op.ussgl493100_delivered_orde_cpe
    AND ussgl498100_upward_adjustm_cpe_sum < op.ussgl498100_upward_adjustm_cpe
    AND obligations_delivered_orde_fyb_sum < op.obligations_delivered_orde_fyb
    AND obligations_delivered_orde_cpe_sum < op.obligations_delivered_orde_cpe
    AND ussgl480200_undelivered_or_fyb_sum < op.ussgl480200_undelivered_or_fyb
    AND ussgl480200_undelivered_or_cpe_sum < op.ussgl480200_undelivered_or_cpe
    AND ussgl483200_undelivered_or_cpe_sum < op.ussgl483200_undelivered_or_cpe
    AND ussgl488200_upward_adjustm_cpe_sum < op.ussgl488200_upward_adjustm_cpe
    AND gross_outlays_undelivered_fyb_sum < op.gross_outlays_undelivered_fyb
    AND gross_outlays_undelivered_cpe_sum < op.gross_outlays_undelivered_cpe
    AND ussgl490200_delivered_orde_cpe_sum < op.ussgl490200_delivered_orde_cpe
    AND ussgl490800_authority_outl_fyb_sum < op.ussgl490800_authority_outl_fyb
    AND ussgl490800_authority_outl_cpe_sum < op.ussgl490800_authority_outl_cpe
    AND ussgl498200_upward_adjustm_cpe_sum < op.ussgl498200_upward_adjustm_cpe
    AND gross_outlays_delivered_or_fyb_sum < op.gross_outlays_delivered_or_fyb
    AND gross_outlays_delivered_or_cpe_sum < op.gross_outlays_delivered_or_cpe
    AND gross_outlay_amount_by_awa_fyb_sum < op.gross_outlay_amount_by_pro_fyb
    AND gross_outlay_amount_by_awa_cpe_sum < op.gross_outlay_amount_by_pro_cpe
    AND obligations_incurred_byawa_cpe_sum < op.obligations_incurred_by_pr_cpe
    AND ussgl487100_downward_adjus_cpe_sum < op.ussgl487100_downward_adjus_cpe
    AND ussgl497100_downward_adjus_cpe_sum < op.ussgl497100_downward_adjus_cpe
    AND ussgl487200_downward_adjus_cpe_sum < op.ussgl487200_downward_adjus_cpe
    AND ussgl497200_downward_adjus_cpe_sum < op.ussgl497200_downward_adjus_cpe
    AND deobligations_recov_by_awa_cpe_sum < op.deobligations_recov_by_pro_cpe;