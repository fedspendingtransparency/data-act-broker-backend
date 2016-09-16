SELECT
	op.row_number,
	SUM(af.ussgl480100_undelivered_or_fyb) AS ussgl480100_undelivered_or_fyb_sum,
	SUM(af.ussgl480100_undelivered_or_cpe) AS ussgl480100_undelivered_or_cpe_sum,
	SUM(af.ussgl483100_undelivered_or_cpe) AS ussgl483100_undelivered_or_cpe_sum,
	SUM(af.ussgl488100_upward_adjustm_cpe) AS ussgl488100_upward_adjustm_cpe_sum,
	SUM(af.obligations_undelivered_or_fyb) AS obligations_undelivered_or_fyb_sum,
	SUM(af.obligations_undelivered_or_cpe) AS obligations_undelivered_or_cpe_sum,
	SUM(af.ussgl490100_delivered_orde_fyb) AS ussgl490100_delivered_orde_fyb_sum,
	SUM(af.ussgl490100_delivered_orde_cpe) AS ussgl490100_delivered_orde_cpe_sum,
	SUM(af.ussgl493100_delivered_orde_cpe) AS ussgl493100_delivered_orde_cpe_sum,
	SUM(af.ussgl498100_upward_adjustm_cpe) AS ussgl498100_upward_adjustm_cpe_sum,
	SUM(af.obligations_delivered_orde_fyb) AS obligations_delivered_orde_fyb_sum,
	SUM(af.obligations_delivered_orde_cpe) AS obligations_delivered_orde_cpe_sum,
	SUM(af.ussgl480200_undelivered_or_fyb) AS ussgl480200_undelivered_or_fyb_sum,
	SUM(af.ussgl480200_undelivered_or_cpe) AS ussgl480200_undelivered_or_cpe_sum,
	SUM(af.ussgl483200_undelivered_or_cpe) AS ussgl483200_undelivered_or_cpe_sum,
	SUM(af.ussgl488200_upward_adjustm_cpe) AS ussgl488200_upward_adjustm_cpe_sum,
	SUM(af.gross_outlays_undelivered_fyb) AS gross_outlays_undelivered_fyb_sum,
	SUM(af.gross_outlays_undelivered_cpe) AS gross_outlays_undelivered_cpe_sum,
	SUM(af.ussgl490200_delivered_orde_cpe) AS ussgl490200_delivered_orde_cpe_sum,
	SUM(af.ussgl490800_authority_outl_fyb) AS ussgl490800_authority_outl_fyb_sum,
	SUM(af.ussgl490800_authority_outl_cpe) AS ussgl490800_authority_outl_cpe_sum,
	SUM(af.ussgl498200_upward_adjustm_cpe) AS ussgl498200_upward_adjustm_cpe_sum,
	SUM(af.gross_outlays_delivered_or_fyb) AS gross_outlays_delivered_or_fyb_sum,
	SUM(af.gross_outlays_delivered_or_cpe) AS gross_outlays_delivered_or_cpe_sum,
	SUM(af.gross_outlay_amount_by_awa_fyb) AS gross_outlay_amount_by_awa_fyb_sum,
	SUM(af.gross_outlay_amount_by_awa_cpe) AS gross_outlay_amount_by_awa_cpe_sum,
	SUM(af.obligations_incurred_byawa_cpe) AS obligations_incurred_byawa_cpe_sum,
	SUM(af.ussgl487100_downward_adjus_cpe) AS ussgl487100_downward_adjus_cpe_sum,
	SUM(af.ussgl497100_downward_adjus_cpe) AS ussgl497100_downward_adjus_cpe_sum,
	SUM(af.ussgl487200_downward_adjus_cpe) AS ussgl487200_downward_adjus_cpe_sum,
	SUM(af.ussgl497200_downward_adjus_cpe) AS ussgl497200_downward_adjus_cpe_sum,
	SUM(af.deobligations_recov_by_awa_cpe) AS deobligations_recov_by_awa_cpe_sum,
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
FROM award_financial AS af
	JOIN object_class_program_activity AS op
		ON af.tas IS NOT DISTINCT FROM op.tas
		AND af.object_class IS NOT DISTINCT FROM op.object_class
		AND af.submission_id = op.submission_id
WHERE af.submission_id = {0}
GROUP BY op.row_number,
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
HAVING SUM(af.ussgl480100_undelivered_or_fyb) < op.ussgl480100_undelivered_or_fyb
    OR SUM(af.ussgl480100_undelivered_or_cpe) < op.ussgl480100_undelivered_or_cpe
    OR SUM(af.ussgl483100_undelivered_or_cpe) < op.ussgl483100_undelivered_or_cpe
    OR SUM(af.ussgl488100_upward_adjustm_cpe) < op.ussgl488100_upward_adjustm_cpe
    OR SUM(af.obligations_undelivered_or_fyb) < op.obligations_undelivered_or_fyb
    OR SUM(af.obligations_undelivered_or_cpe) < op.obligations_undelivered_or_cpe
    OR SUM(af.ussgl490100_delivered_orde_fyb) < op.ussgl490100_delivered_orde_fyb
    OR SUM(af.ussgl490100_delivered_orde_cpe) < op.ussgl490100_delivered_orde_cpe
    OR SUM(af.ussgl493100_delivered_orde_cpe) < op.ussgl493100_delivered_orde_cpe
    OR SUM(af.ussgl498100_upward_adjustm_cpe) < op.ussgl498100_upward_adjustm_cpe
    OR SUM(af.obligations_delivered_orde_fyb) < op.obligations_delivered_orde_fyb
    OR SUM(af.obligations_delivered_orde_cpe) < op.obligations_delivered_orde_cpe
    OR SUM(af.ussgl480200_undelivered_or_fyb) < op.ussgl480200_undelivered_or_fyb
    OR SUM(af.ussgl480200_undelivered_or_cpe) < op.ussgl480200_undelivered_or_cpe
    OR SUM(af.ussgl483200_undelivered_or_cpe) < op.ussgl483200_undelivered_or_cpe
    OR SUM(af.ussgl488200_upward_adjustm_cpe) < op.ussgl488200_upward_adjustm_cpe
    OR SUM(af.gross_outlays_undelivered_fyb) < op.gross_outlays_undelivered_fyb
    OR SUM(af.gross_outlays_undelivered_cpe) < op.gross_outlays_undelivered_cpe
    OR SUM(af.ussgl490200_delivered_orde_cpe) < op.ussgl490200_delivered_orde_cpe
    OR SUM(af.ussgl490800_authority_outl_fyb) < op.ussgl490800_authority_outl_fyb
    OR SUM(af.ussgl490800_authority_outl_cpe) < op.ussgl490800_authority_outl_cpe
    OR SUM(af.ussgl498200_upward_adjustm_cpe) < op.ussgl498200_upward_adjustm_cpe
    OR SUM(af.gross_outlays_delivered_or_fyb) < op.gross_outlays_delivered_or_fyb
    OR SUM(af.gross_outlays_delivered_or_cpe) < op.gross_outlays_delivered_or_cpe
    OR SUM(af.gross_outlay_amount_by_awa_fyb) < op.gross_outlay_amount_by_pro_fyb
    OR SUM(af.gross_outlay_amount_by_awa_cpe) < op.gross_outlay_amount_by_pro_cpe
    OR SUM(af.obligations_incurred_byawa_cpe) < op.obligations_incurred_by_pr_cpe
    OR SUM(af.ussgl487100_downward_adjus_cpe) < op.ussgl487100_downward_adjus_cpe
    OR SUM(af.ussgl497100_downward_adjus_cpe) < op.ussgl497100_downward_adjus_cpe
    OR SUM(af.ussgl487200_downward_adjus_cpe) < op.ussgl487200_downward_adjus_cpe
    OR SUM(af.ussgl497200_downward_adjus_cpe) < op.ussgl497200_downward_adjus_cpe
    OR SUM(af.deobligations_recov_by_awa_cpe) < op.deobligations_recov_by_pro_cpe;