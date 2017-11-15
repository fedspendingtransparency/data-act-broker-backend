-- Reimbursable flag indicator is required when reporting obligation or outlay USSGL account balances
-- (excluding downward adjustments SGL accounts).
SELECT
    af.row_number,
    af.by_direct_reimbursable_fun
FROM award_financial AS af
WHERE af.submission_id = {0}
    AND (ussgl480100_undelivered_or_fyb IS NOT NULL
        OR ussgl480100_undelivered_or_cpe IS NOT NULL
        OR ussgl488100_upward_adjustm_cpe IS NOT NULL
        OR ussgl490100_delivered_orde_fyb IS NOT NULL
        OR ussgl490100_delivered_orde_cpe IS NOT NULL
        OR ussgl498100_upward_adjustm_cpe IS NOT NULL
        OR ussgl480200_undelivered_or_fyb IS NOT NULL
        OR ussgl480200_undelivered_or_cpe IS NOT NULL
        OR ussgl488200_upward_adjustm_cpe IS NOT NULL
        OR ussgl490200_delivered_orde_cpe IS NOT NULL
        OR ussgl490800_authority_outl_fyb IS NOT NULL
        OR ussgl490800_authority_outl_cpe IS NOT NULL
        OR ussgl498200_upward_adjustm_cpe IS NOT NULL)
    AND COALESCE(by_direct_reimbursable_fun, '') = '';
