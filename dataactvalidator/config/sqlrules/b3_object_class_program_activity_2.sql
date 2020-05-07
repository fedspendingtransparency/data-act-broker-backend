-- ObligationsUndeliveredOrdersUnpaidTotal (CPE) = USSGL(4801 + 4831 + 4881) for the same date context and TAS/
-- Disaster Emergency Fund Code (DEFC) combination. This applies to the program activity and object class level.
SELECT
    row_number,
    obligations_undelivered_or_cpe,
    ussgl480100_undelivered_or_cpe,
    ussgl483100_undelivered_or_cpe,
    ussgl488100_upward_adjustm_cpe,
    COALESCE(obligations_undelivered_or_cpe, 0) - (COALESCE(ussgl480100_undelivered_or_cpe, 0) +
                                                   COALESCE(ussgl483100_undelivered_or_cpe, 0) +
                                                   COALESCE(ussgl488100_upward_adjustm_cpe, 0)) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DEFC",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND COALESCE(obligations_undelivered_or_cpe, 0) <>
        COALESCE(ussgl480100_undelivered_or_cpe, 0) +
        COALESCE(ussgl483100_undelivered_or_cpe, 0) +
        COALESCE(ussgl488100_upward_adjustm_cpe, 0);
