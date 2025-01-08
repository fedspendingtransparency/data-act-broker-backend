-- ObligationsDeliveredOrdersUnpaidTotal (CPE) = USSGL(4901 + 490110 + 4931 + 4981) for the same TAS/DEFC combination
-- where PYA = "X". This applies to the program activity and object class level.
SELECT
    row_number,
    prior_year_adjustment,
    obligations_delivered_orde_cpe,
    ussgl490100_delivered_orde_cpe,
    ussgl490110_rein_deliv_ord_cpe,
    ussgl493100_delivered_orde_cpe,
    ussgl498100_upward_adjustm_cpe,
    COALESCE(obligations_delivered_orde_cpe, 0) - (COALESCE(ussgl490100_delivered_orde_cpe, 0) +
                                                   COALESCE(ussgl490110_rein_deliv_ord_cpe, 0) +
                                                   COALESCE(ussgl493100_delivered_orde_cpe, 0) +
                                                   COALESCE(ussgl498100_upward_adjustm_cpe, 0)) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    program_activity_name AS "uniqueid_ProgramActivityName",
    object_class AS "uniqueid_ObjectClass",
    by_direct_reimbursable_fun AS "uniqueid_ByDirectReimbursableFundingSource"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND UPPER(prior_year_adjustment) = 'X'
    AND COALESCE(obligations_delivered_orde_cpe, 0) <>
        COALESCE(ussgl490100_delivered_orde_cpe, 0) +
        COALESCE(ussgl490110_rein_deliv_ord_cpe, 0) +
        COALESCE(ussgl493100_delivered_orde_cpe, 0) +
        COALESCE(ussgl498100_upward_adjustm_cpe, 0);
