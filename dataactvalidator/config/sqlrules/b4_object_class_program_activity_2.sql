-- ObligationsDeliveredOrdersUnpaidTotal (CPE) = USSGL(4901 + 4931 + 4981) for the same date context and TAS/DEFC
-- combination. This applies to the program activity and object class level.
SELECT
    row_number,
    obligations_delivered_orde_cpe,
    ussgl490100_delivered_orde_cpe,
    ussgl493100_delivered_orde_cpe,
    ussgl498100_upward_adjustm_cpe,
    COALESCE(obligations_delivered_orde_cpe, 0) - (COALESCE(ussgl490100_delivered_orde_cpe, 0) +
                                                   COALESCE(ussgl493100_delivered_orde_cpe, 0) +
                                                   COALESCE(ussgl498100_upward_adjustm_cpe, 0)) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DEFC",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND COALESCE(obligations_delivered_orde_cpe, 0) <>
        COALESCE(ussgl490100_delivered_orde_cpe, 0) +
        COALESCE(ussgl493100_delivered_orde_cpe, 0) +
        COALESCE(ussgl498100_upward_adjustm_cpe, 0);
