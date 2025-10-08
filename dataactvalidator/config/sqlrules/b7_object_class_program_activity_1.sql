-- GrossOutlaysDeliveredOrdersPaidTotal (FYB) = USSGL 4908 for the unique combination defined in Rule B19 where
-- PYA = "X". Note for FYB values, only 4908 is expected to have a balance other than zero.
SELECT
    row_number,
    prior_year_adjustment,
    gross_outlays_delivered_or_fyb,
    ussgl490800_authority_outl_fyb,
    COALESCE(gross_outlays_delivered_or_fyb, 0) - COALESCE(ussgl490800_authority_outl_fyb, 0) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    program_activity_name AS "uniqueid_ProgramActivityName",
    object_class AS "uniqueid_ObjectClass",
    by_direct_reimbursable_fun AS "uniqueid_ByDirectReimbursableFundingSource"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND UPPER(prior_year_adjustment) = 'X'
    AND COALESCE(gross_outlays_delivered_or_fyb, 0) <> COALESCE(ussgl490800_authority_outl_fyb, 0);
