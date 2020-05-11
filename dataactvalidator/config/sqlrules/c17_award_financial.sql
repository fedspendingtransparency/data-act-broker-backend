-- TransactionObligatedAmount and USSGL related balances and subtotals cannot be provided on the same row.
SELECT
    row_number,
    transaction_obligated_amou,
    display_tas AS "uniqueid_TAS",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(transaction_obligated_amou, 0) > 0
    AND (
        COALESCE(ussgl480100_undelivered_or_cpe, 0) > 0
        OR COALESCE(ussgl480100_undelivered_or_fyb, 0) > 0
        OR COALESCE(ussgl480200_undelivered_or_cpe, 0) > 0
        OR COALESCE(ussgl480200_undelivered_or_fyb, 0) > 0
        OR COALESCE(ussgl483100_undelivered_or_cpe, 0) > 0
        OR COALESCE(ussgl483200_undelivered_or_cpe, 0) > 0
        OR COALESCE(ussgl487100_downward_adjus_cpe, 0) > 0
        OR COALESCE(ussgl487200_downward_adjus_cpe, 0) > 0
        OR COALESCE(ussgl488100_upward_adjustm_cpe, 0) > 0
        OR COALESCE(ussgl488200_upward_adjustm_cpe, 0) > 0
        OR COALESCE(ussgl490100_delivered_orde_cpe, 0) > 0
        OR COALESCE(ussgl490100_delivered_orde_fyb, 0) > 0
        OR COALESCE(ussgl490200_delivered_orde_cpe, 0) > 0
        OR COALESCE(ussgl490800_authority_outl_cpe, 0) > 0
        OR COALESCE(ussgl490800_authority_outl_fyb, 0) > 0
        OR COALESCE(ussgl493100_delivered_orde_cpe, 0) > 0
        OR COALESCE(ussgl497100_downward_adjus_cpe, 0) > 0
        OR COALESCE(ussgl497200_downward_adjus_cpe, 0) > 0
        OR COALESCE(ussgl498100_upward_adjustm_cpe, 0) > 0
        OR COALESCE(ussgl498200_upward_adjustm_cpe, 0) > 0
        OR COALESCE(gross_outlay_amount_by_awa_cpe, 0) > 0
        OR COALESCE(gross_outlay_amount_by_awa_fyb, 0) > 0
        OR COALESCE(gross_outlays_delivered_or_cpe, 0) > 0
        OR COALESCE(gross_outlays_delivered_or_fyb, 0) > 0
        OR COALESCE(gross_outlays_undelivered_cpe, 0) > 0
        OR COALESCE(gross_outlays_undelivered_fyb, 0) > 0
        OR COALESCE(obligations_delivered_orde_cpe, 0) > 0
        OR COALESCE(obligations_delivered_orde_fyb, 0) > 0
        OR COALESCE(obligations_incurred_byawa_cpe, 0) > 0
        OR COALESCE(obligations_undelivered_or_cpe, 0) > 0
        OR COALESCE(obligations_undelivered_or_fyb, 0) > 0
    );
