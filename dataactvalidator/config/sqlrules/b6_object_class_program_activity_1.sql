-- GrossOutlaysUndeliveredOrdersPrepaidTotal (FYB) = USSGL 4802 for the same TAS/DEFC combination where PYA = "X".
-- This applies to the program activity and object class level.
SELECT
    row_number,
    prior_year_adjustment,
    gross_outlays_undelivered_fyb,
    ussgl480200_undelivered_or_fyb,
    COALESCE(gross_outlays_undelivered_fyb, 0) - COALESCE(ussgl480200_undelivered_or_fyb, 0) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    program_activity_name AS "uniqueid_ProgramActivityName",
    object_class AS "uniqueid_ObjectClass",
    by_direct_reimbursable_fun AS "uniqueid_ByDirectReimbursableFundingSource"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND UPPER(prior_year_adjustment) = 'X'
    AND COALESCE(gross_outlays_undelivered_fyb, 0) <> COALESCE(ussgl480200_undelivered_or_fyb, 0);
