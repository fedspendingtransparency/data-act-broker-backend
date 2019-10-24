-- Reimbursable flag indicator is required when reporting obligation or outlay USSGL account balances
-- (excluding downward adjustments SGL accounts).
SELECT
    row_number,
    by_direct_reimbursable_fun,
    tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM award_financial
WHERE submission_id = {0}
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
        OR ussgl498200_upward_adjustm_cpe IS NOT NULL
    )
    AND COALESCE(by_direct_reimbursable_fun, '') = '';
