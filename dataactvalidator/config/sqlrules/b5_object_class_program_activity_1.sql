-- File B (object class program activity): GrossOutlayAmountByProgramObjectClass_FYB =
-- GrossOutlaysUndeliveredOrdersPrepaidTotal_FYB + GrossOutlaysDeliveredOrdersPaidTotal_FYB.
SELECT
    row_number,
    gross_outlay_amount_by_pro_fyb,
    gross_outlays_undelivered_fyb,
    gross_outlays_delivered_or_fyb,
    COALESCE(gross_outlay_amount_by_pro_fyb, 0) - (COALESCE(gross_outlays_undelivered_fyb, 0) +
                                                   COALESCE(gross_outlays_delivered_or_fyb, 0)) AS "difference",
    tas AS "uniqueid_TAS",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND COALESCE(gross_outlay_amount_by_pro_fyb, 0) <>
        COALESCE(gross_outlays_undelivered_fyb, 0) +
        COALESCE(gross_outlays_delivered_or_fyb, 0);
