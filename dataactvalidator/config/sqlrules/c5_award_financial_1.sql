-- GrossOutlayAmountByAward (FYB, File C (award financial)) = GrossOutlaysUndeliveredOrdersPrepaidTotal (FYB, File C) +
-- GrossOutlaysDeliveredOrdersPaidTotal (FYB, File C)
SELECT
    row_number,
    gross_outlay_amount_by_awa_fyb,
    gross_outlays_undelivered_fyb,
    gross_outlays_delivered_or_fyb,
    COALESCE(gross_outlay_amount_by_awa_fyb, 0) - (COALESCE(gross_outlays_undelivered_fyb, 0) +
                                                   COALESCE(gross_outlays_delivered_or_fyb, 0)) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    prior_year_adjustment AS "uniqueid_PriorYearAdjustment",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(gross_outlay_amount_by_awa_fyb, 0) <>
        COALESCE(gross_outlays_undelivered_fyb, 0) +
        COALESCE(gross_outlays_delivered_or_fyb, 0);
