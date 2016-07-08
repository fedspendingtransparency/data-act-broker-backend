SELECT
    op.row_number,
    op.by_direct_reimbursable_fun
FROM object_class_program_activity as op
WHERE submission_id = {} AND (
    ussgl480100_undelivered_or_fyb IS NOT NULL
    OR ussgl480100_undelivered_or_cpe IS NOT NULL
    OR ussgl483100_undelivered_or_cpe IS NOT NULL
    OR ussgl488100_upward_adjustm_cpe IS NOT NULL
    OR ussgl490100_delivered_orde_fyb IS NOT NULL
    OR ussgl490100_delivered_orde_cpe IS NOT NULL
    OR ussgl493100_delivered_orde_cpe IS NOT NULL
    OR ussgl498100_upward_adjustm_cpe IS NOT NULL
    OR ussgl480200_undelivered_or_cpe IS NOT NULL
    OR ussgl480200_undelivered_or_fyb IS NOT NULL
    OR ussgl483200_undelivered_or_cpe IS NOT NULL
    OR ussgl488200_upward_adjustm_cpe IS NOT NULL
    OR ussgl490200_delivered_orde_cpe IS NOT NULL
    OR ussgl490800_authority_outl_fyb IS NOT NULL
    OR ussgl490800_authority_outl_cpe IS NOT NULL
    OR ussgl498200_upward_adjustm_cpe IS NOT NULL )
    AND COALESCE(by_direct_reimbursable_fun, '') = ''
