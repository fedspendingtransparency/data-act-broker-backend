-- GrossOutlayAmountByProgramObjectClass_FYB (File B) = GrossOutlaysUndeliveredOrdersPrepaidTotal_FYB (File B) +
-- GrossOutlaysDeliveredOrdersPaidTotal_FYB (File B).
SELECT
    row_number,
    gross_outlay_amount_by_pro_fyb,
    gross_outlays_undelivered_fyb,
    gross_outlays_delivered_or_fyb
FROM object_class_program_activity
WHERE submission_id = {0}
    AND COALESCE(gross_outlay_amount_by_pro_fyb, 0) <>
        COALESCE(gross_outlays_undelivered_fyb, 0) +
        COALESCE(gross_outlays_delivered_or_fyb, 0);
