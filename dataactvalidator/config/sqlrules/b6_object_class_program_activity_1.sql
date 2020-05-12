-- GrossOutlaysUndeliveredOrdersPrepaidTotal (FYB) = USSGL 4802 for the same TAS/DEFC combination.
-- This applies to the program activity and object class level.
SELECT
    row_number,
    gross_outlays_undelivered_fyb,
    ussgl480200_undelivered_or_fyb,
    COALESCE(gross_outlays_undelivered_fyb, 0) - COALESCE(ussgl480200_undelivered_or_fyb, 0) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND COALESCE(gross_outlays_undelivered_fyb, 0) <> COALESCE(ussgl480200_undelivered_or_fyb, 0);
