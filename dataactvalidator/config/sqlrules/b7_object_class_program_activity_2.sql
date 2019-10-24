-- GrossOutlaysDeliveredOrdersPaidTotal (CPE)= USSGL(4902 + 4908 + 4982). This applies to the program activity
-- and object class level.
SELECT
    row_number,
    gross_outlays_delivered_or_cpe,
    ussgl490200_delivered_orde_cpe,
    ussgl490800_authority_outl_cpe,
    ussgl498200_upward_adjustm_cpe,
    COALESCE(gross_outlays_delivered_or_cpe, 0) - (COALESCE(ussgl490200_delivered_orde_cpe, 0) +
                                                   COALESCE(ussgl490800_authority_outl_cpe, 0) +
                                                   COALESCE(ussgl498200_upward_adjustm_cpe, 0)) AS "difference",
    tas AS "uniqueid_TAS",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND COALESCE(gross_outlays_delivered_or_cpe, 0) <>
        COALESCE(ussgl490200_delivered_orde_cpe, 0) +
        COALESCE(ussgl490800_authority_outl_cpe, 0) +
        COALESCE(ussgl498200_upward_adjustm_cpe, 0);
