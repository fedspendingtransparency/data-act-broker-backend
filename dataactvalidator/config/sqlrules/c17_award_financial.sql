-- TransactionObligatedAmount and USSGL related balances and subtotals cannot be provided on the same row.
-- Please note that this rule will apply for any non-null (non-blank) value provided, including zero (0).
SELECT
    row_number,
    transaction_obligated_amou,
    display_tas AS "uniqueid_TAS",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode"
FROM award_financial
WHERE submission_id = {0}
    AND transaction_obligated_amou IS NOT NULL
    AND (
        ussgl480100_undelivered_or_cpe IS NOT NULL
        OR ussgl480100_undelivered_or_fyb IS NOT NULL
        OR ussgl480200_undelivered_or_cpe IS NOT NULL
        OR ussgl480200_undelivered_or_fyb IS NOT NULL
        OR ussgl480210_rein_undel_obs_cpe IS NOT NULL
        OR ussgl483100_undelivered_or_cpe IS NOT NULL
        OR ussgl483200_undelivered_or_cpe IS NOT NULL
        OR ussgl487100_downward_adjus_cpe IS NOT NULL
        OR ussgl487200_downward_adjus_cpe IS NOT NULL
        OR ussgl488100_upward_adjustm_cpe IS NOT NULL
        OR ussgl488200_upward_adjustm_cpe IS NOT NULL
        OR ussgl490100_delivered_orde_cpe IS NOT NULL
        OR ussgl490100_delivered_orde_fyb IS NOT NULL
        OR ussgl490200_delivered_orde_cpe IS NOT NULL
        OR ussgl490800_authority_outl_cpe IS NOT NULL
        OR ussgl490800_authority_outl_fyb IS NOT NULL
        OR ussgl493100_delivered_orde_cpe IS NOT NULL
        OR ussgl497100_downward_adjus_cpe IS NOT NULL
        OR ussgl497200_downward_adjus_cpe IS NOT NULL
        OR ussgl497210_down_adj_refun_cpe IS NOT NULL
        OR ussgl498100_upward_adjustm_cpe IS NOT NULL
        OR ussgl498200_upward_adjustm_cpe IS NOT NULL
        OR gross_outlay_amount_by_awa_cpe IS NOT NULL
        OR gross_outlay_amount_by_awa_fyb IS NOT NULL
        OR gross_outlays_delivered_or_cpe IS NOT NULL
        OR gross_outlays_delivered_or_fyb IS NOT NULL
        OR gross_outlays_undelivered_cpe IS NOT NULL
        OR gross_outlays_undelivered_fyb IS NOT NULL
        OR obligations_delivered_orde_cpe IS NOT NULL
        OR obligations_delivered_orde_fyb IS NOT NULL
        OR obligations_incurred_byawa_cpe IS NOT NULL
        OR obligations_undelivered_or_cpe IS NOT NULL
        OR obligations_undelivered_or_fyb IS NOT NULL
        OR deobligations_recov_by_awa_cpe IS NOT NULL
    );
