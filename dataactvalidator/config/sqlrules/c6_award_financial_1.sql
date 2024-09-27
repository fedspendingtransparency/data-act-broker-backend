-- GrossOutlaysUndeliveredOrdersPrepaidTotal (FYB) = USSGL(4802). This applies to the award level.
SELECT
    row_number,
    gross_outlays_undelivered_fyb,
    ussgl480200_undelivered_or_fyb,
    COALESCE(gross_outlays_undelivered_fyb, 0) - COALESCE(ussgl480200_undelivered_or_fyb, 0) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    prior_year_adjustment AS "uniqueid_PriorYearAdjustment",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(gross_outlays_undelivered_fyb, 0) <> COALESCE(ussgl480200_undelivered_or_fyb, 0);
