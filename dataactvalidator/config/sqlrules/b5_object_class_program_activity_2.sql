-- File B (object class program activity): GrossOutlayAmountByProgramObjectClass_CPE =
-- GrossOutlaysUndeliveredOrdersPrepaidTotal_CPE - GrossOutlaysUndeliveredOrdersPrepaidTotal_FYB +
-- GrossOutlaysDeliveredOrdersPaidTotal_CPE - GrossOutlaysDeliveredOrdersPaidTotal_FYB
-- for the same TAS/DEFC combination where PYA = "X".
SELECT
    row_number,
    prior_year_adjustment,
    gross_outlay_amount_by_pro_cpe,
    gross_outlays_undelivered_cpe,
    gross_outlays_delivered_or_cpe,
    gross_outlays_undelivered_fyb,
    gross_outlays_delivered_or_fyb,
    COALESCE(gross_outlay_amount_by_pro_cpe, 0) - (COALESCE(gross_outlays_undelivered_cpe, 0) -
                                                   COALESCE(gross_outlays_undelivered_fyb, 0) +
                                                   COALESCE(gross_outlays_delivered_or_cpe, 0) -
                                                   COALESCE(gross_outlays_delivered_or_fyb, 0)) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND COALESCE(UPPER(prior_year_adjustment), '') = 'X'
    AND COALESCE(gross_outlay_amount_by_pro_cpe, 0) <>
        COALESCE(gross_outlays_undelivered_cpe, 0) -
        COALESCE(gross_outlays_undelivered_fyb, 0) +
        COALESCE(gross_outlays_delivered_or_cpe, 0) -
        COALESCE(gross_outlays_delivered_or_fyb, 0);
