-- Reimbursable flag indicator is required when reporting non-zero obligation or outlay USSGL account balances.
SELECT
    row_number,
    object_class,
    by_direct_reimbursable_fun,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND (COALESCE(ussgl480100_undelivered_or_fyb, 0) <> 0
        OR COALESCE(ussgl480100_undelivered_or_cpe, 0) <> 0
        OR COALESCE(ussgl480110_rein_undel_ord_cpe, 0) <> 0
        OR COALESCE(ussgl480200_undelivered_or_cpe, 0) <> 0
        OR COALESCE(ussgl480200_undelivered_or_fyb, 0) <> 0
        OR COALESCE(ussgl480210_rein_undel_obs_cpe, 0) <> 0
        OR COALESCE(ussgl483100_undelivered_or_cpe, 0) <> 0
        OR COALESCE(ussgl483200_undelivered_or_cpe, 0) <> 0
        OR COALESCE(ussgl487100_downward_adjus_cpe, 0) <> 0
        OR COALESCE(ussgl487200_downward_adjus_cpe, 0) <> 0
        OR COALESCE(ussgl488100_upward_adjustm_cpe, 0) <> 0
        OR COALESCE(ussgl488200_upward_adjustm_cpe, 0) <> 0
        OR COALESCE(ussgl490100_delivered_orde_fyb, 0) <> 0
        OR COALESCE(ussgl490100_delivered_orde_cpe, 0) <> 0
        OR COALESCE(ussgl490110_rein_deliv_ord_cpe, 0) <> 0
        OR COALESCE(ussgl490200_delivered_orde_cpe, 0) <> 0
        OR COALESCE(ussgl490800_authority_outl_fyb, 0) <> 0
        OR COALESCE(ussgl490800_authority_outl_cpe, 0) <> 0
        OR COALESCE(ussgl493100_delivered_orde_cpe, 0) <> 0
        OR COALESCE(ussgl497100_downward_adjus_cpe, 0) <> 0
        OR COALESCE(ussgl497200_downward_adjus_cpe, 0) <> 0
        OR COALESCE(ussgl497210_down_adj_refun_cpe, 0) <> 0
        OR COALESCE(ussgl498100_upward_adjustm_cpe, 0) <> 0
        OR COALESCE(ussgl498200_upward_adjustm_cpe, 0) <> 0
    )
    AND COALESCE(by_direct_reimbursable_fun, '') = '';
