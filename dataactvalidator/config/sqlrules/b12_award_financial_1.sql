-- Reimbursable flag indicator is required when reporting non-zero obligation or outlay USSGL account balances
-- (excluding USSGL accounts for downward adjustments and transfers).
SELECT
    row_number,
    by_direct_reimbursable_fun,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM award_financial
WHERE submission_id = {0}
    AND (COALESCE(ussgl480100_undelivered_or_fyb, 0) <> 0
        OR COALESCE(ussgl480100_undelivered_or_cpe, 0) <> 0
        OR COALESCE(ussgl488100_upward_adjustm_cpe, 0) <> 0
        OR COALESCE(ussgl490100_delivered_orde_fyb, 0) <> 0
        OR COALESCE(ussgl490100_delivered_orde_cpe, 0) <> 0
        OR COALESCE(ussgl498100_upward_adjustm_cpe, 0) <> 0
        OR COALESCE(ussgl480200_undelivered_or_fyb, 0) <> 0
        OR COALESCE(ussgl480200_undelivered_or_cpe, 0) <> 0
        OR COALESCE(ussgl488200_upward_adjustm_cpe, 0) <> 0
        OR COALESCE(ussgl490200_delivered_orde_cpe, 0) <> 0
        OR COALESCE(ussgl490800_authority_outl_fyb, 0) <> 0
        OR COALESCE(ussgl490800_authority_outl_cpe, 0) <> 0
        OR COALESCE(ussgl498200_upward_adjustm_cpe, 0) <> 0
    )
    AND COALESCE(by_direct_reimbursable_fun, '') = '';
