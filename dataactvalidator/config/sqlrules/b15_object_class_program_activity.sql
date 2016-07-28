SELECT
    op.row_number,
    op.ussgl480100_undelivered_or_cpe,
    op.ussgl480100_undelivered_or_fyb,
    op.ussgl480200_undelivered_or_cpe,
    op.ussgl480200_undelivered_or_fyb,
    op.ussgl488100_upward_adjustm_cpe,
    op.ussgl488200_upward_adjustm_cpe,
    op.ussgl490100_delivered_orde_cpe,
    op.ussgl490100_delivered_orde_fyb,
    op.ussgl490200_delivered_orde_cpe,
    op.ussgl490800_authority_outl_cpe,
    op.ussgl490800_authority_outl_fyb,
    op.ussgl498100_upward_adjustm_cpe,
    op.ussgl498200_upward_adjustm_cpe,
    sf.amount
FROM object_class_program_activity as op
    INNER JOIN sf_133 as sf ON op.tas = sf.tas
WHERE op.submission_id = {} AND
    sf.line = 2104 AND
    LOWER(op.by_direct_reimbursable_fun) = 'r' AND
    (
        (COALESCE(op.ussgl480100_undelivered_or_cpe, 0) - COALESCE(op.ussgl480100_undelivered_or_fyb, 0)) +
        (COALESCE(op.ussgl480200_undelivered_or_cpe, 0) - COALESCE(op.ussgl480200_undelivered_or_fyb, 0)) +
        COALESCE(op.ussgl488100_upward_adjustm_cpe, 0) +
        COALESCE(op.ussgl488200_upward_adjustm_cpe, 0) +
        (COALESCE(op.ussgl490100_delivered_orde_cpe, 0) - COALESCE(op.ussgl490100_delivered_orde_fyb, 0)) +
        COALESCE(op.ussgl490200_delivered_orde_cpe, 0) +
        (COALESCE(op.ussgl490800_authority_outl_cpe, 0) - COALESCE(op.ussgl490800_authority_outl_fyb, 0)) +
        COALESCE(op.ussgl498100_upward_adjustm_cpe, 0) +
        COALESCE(op.ussgl498200_upward_adjustm_cpe, 0)
    ) <> sf.amount