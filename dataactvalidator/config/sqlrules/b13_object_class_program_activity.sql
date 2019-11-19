-- DeobligationsRecoveriesRefundsdOfPriorYearByProgramObjectClass_CPE = USSGL(4871+ 4872 + 4971 + 4972)
SELECT
    row_number,
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
    program_activity_code AS "uniqueid_ProgramActivityCode",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND COALESCE(deobligations_recov_by_pro_cpe, 0) <>
        COALESCE(ussgl487100_downward_adjus_cpe, 0) +
        COALESCE(ussgl487200_downward_adjus_cpe, 0) +
        COALESCE(ussgl497100_downward_adjus_cpe, 0) +
        COALESCE(ussgl497200_downward_adjus_cpe, 0);
