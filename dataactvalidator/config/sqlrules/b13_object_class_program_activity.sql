-- DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE in File B = USSGL(4871+ 4872 + 4971 + 4972)
-- in File B for the same reporting period and TAS and DEFC combination where PYA = "X".
SELECT
    row_number,
    prior_year_adjustment,
    deobligations_recov_by_pro_cpe,
    ussgl487100_downward_adjus_cpe,
    ussgl487200_downward_adjus_cpe,
    ussgl497100_downward_adjus_cpe,
    ussgl497200_downward_adjus_cpe,
    COALESCE(deobligations_recov_by_pro_cpe, 0) - (COALESCE(ussgl487100_downward_adjus_cpe, 0) +
                                                   COALESCE(ussgl487200_downward_adjus_cpe, 0) +
                                                   COALESCE(ussgl497100_downward_adjus_cpe, 0) +
                                                   COALESCE(ussgl497200_downward_adjus_cpe, 0)) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND UPPER(prior_year_adjustment) = 'X'
    AND COALESCE(deobligations_recov_by_pro_cpe, 0) <>
        COALESCE(ussgl487100_downward_adjus_cpe, 0) +
        COALESCE(ussgl487200_downward_adjus_cpe, 0) +
        COALESCE(ussgl497100_downward_adjus_cpe, 0) +
        COALESCE(ussgl497200_downward_adjus_cpe, 0);
