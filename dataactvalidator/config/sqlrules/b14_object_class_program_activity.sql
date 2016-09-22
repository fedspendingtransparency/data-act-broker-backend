SELECT
    DISTINCT NULL as row_number,
    op.tas,
    SUM(op.ussgl480100_undelivered_or_cpe) as ussgl480100_undelivered_or_cpe_sum,
    SUM(op.ussgl480100_undelivered_or_fyb) as ussgl480100_undelivered_or_fyb_sum,
    SUM(op.ussgl480200_undelivered_or_cpe) as ussgl480200_undelivered_or_cpe_sum,
    SUM(op.ussgl480200_undelivered_or_fyb) as ussgl480200_undelivered_or_fyb_sum,
    SUM(op.ussgl488100_upward_adjustm_cpe) as ussgl488100_upward_adjustm_cpe_sum,
    SUM(op.ussgl488200_upward_adjustm_cpe) as ussgl488200_upward_adjustm_cpe_sum,
    SUM(op.ussgl490100_delivered_orde_cpe) as ussgl490100_delivered_orde_cpe_sum,
    SUM(op.ussgl490100_delivered_orde_fyb) as ussgl490100_delivered_orde_fyb_sum,
    SUM(op.ussgl490200_delivered_orde_cpe) as ussgl490200_delivered_orde_cpe_sum,
    SUM(op.ussgl490800_authority_outl_cpe) as ussgl490800_authority_outl_cpe_sum,
    SUM(op.ussgl490800_authority_outl_fyb) as ussgl490800_authority_outl_fyb_sum,
    SUM(op.ussgl498100_upward_adjustm_cpe) as ussgl498100_upward_adjustm_cpe_sum,
    SUM(op.ussgl498200_upward_adjustm_cpe) as ussgl498200_upward_adjustm_cpe_sum,
    sf.amount as sf_133_amount
FROM object_class_program_activity as op
    INNER JOIN sf_133 as sf ON op.tas = sf.tas
    INNER JOIN submission as sub ON op.submission_id = sub.submission_id AND
        sf.period = sub.reporting_fiscal_period AND
        sf.fiscal_year = sub.reporting_fiscal_year
WHERE op.submission_id = {} AND sf.line = 2004 AND
    LOWER(op.by_direct_reimbursable_fun) = 'd'
GROUP BY op.tas, sf.amount
HAVING
    (
        (SUM(ussgl480100_undelivered_or_cpe) - SUM(ussgl480100_undelivered_or_fyb)) +
        (SUM(ussgl480200_undelivered_or_cpe) - SUM(ussgl480200_undelivered_or_fyb)) +
        SUM(ussgl488100_upward_adjustm_cpe) +
        SUM(ussgl488200_upward_adjustm_cpe) +
        (SUM(ussgl490100_delivered_orde_cpe) - SUM(ussgl490100_delivered_orde_fyb)) +
        SUM(ussgl490200_delivered_orde_cpe) +
        (SUM(ussgl490800_authority_outl_cpe) - SUM(ussgl490800_authority_outl_fyb)) +
        SUM(ussgl498100_upward_adjustm_cpe) +
        SUM(ussgl498200_upward_adjustm_cpe)
    ) <> sf.amount