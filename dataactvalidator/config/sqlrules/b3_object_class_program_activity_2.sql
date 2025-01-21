-- ObligationsUndeliveredOrdersUnpaidTotal (CPE) = USSGL(4801 + 480110 + 4831 + 4881) for the same TAS/
-- Disaster Emergency Fund Code (DEFC) combination where PYA = "X". This applies to the program activity and
-- object class level.
SELECT
    row_number,
    prior_year_adjustment,
    obligations_undelivered_or_cpe,
    ussgl480100_undelivered_or_cpe,
    ussgl480110_rein_undel_ord_cpe,
    ussgl483100_undelivered_or_cpe,
    ussgl488100_upward_adjustm_cpe,
    COALESCE(obligations_undelivered_or_cpe, 0) - (COALESCE(ussgl480100_undelivered_or_cpe, 0) +
                                                   COALESCE(ussgl480110_rein_undel_ord_cpe, 0) +
                                                   COALESCE(ussgl483100_undelivered_or_cpe, 0) +
                                                   COALESCE(ussgl488100_upward_adjustm_cpe, 0)) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    program_activity_name AS "uniqueid_ProgramActivityName",
    object_class AS "uniqueid_ObjectClass",
    by_direct_reimbursable_fun AS "uniqueid_ByDirectReimbursableFundingSource"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND UPPER(prior_year_adjustment) = 'X'
    AND COALESCE(obligations_undelivered_or_cpe, 0) <>
        COALESCE(ussgl480100_undelivered_or_cpe, 0) +
        COALESCE(ussgl480110_rein_undel_ord_cpe, 0) +
        COALESCE(ussgl483100_undelivered_or_cpe, 0) +
        COALESCE(ussgl488100_upward_adjustm_cpe, 0);
