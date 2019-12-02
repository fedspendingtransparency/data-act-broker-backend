-- GrossOutlayAmountByAward (CPE, File C (award financial)) = (GrossOutlaysUndeliveredOrdersPrepaidTotal (CPE, File C) -
-- GrossOutlaysUndeliveredOrdersPrepaidTotal (FYB, File C)) + (GrossOutlaysDeliveredOrdersPaidTotal (CPE, File C) -
-- GrossOutlaysDeliveredOrdersPaidTotal (FYB, File C))
SELECT
    row_number,
    gross_outlay_amount_by_awa_cpe,
    gross_outlays_undelivered_cpe,
    gross_outlays_delivered_or_cpe,
    COALESCE(gross_outlay_amount_by_awa_cpe, 0) - ((COALESCE(gross_outlays_undelivered_cpe, 0) -
                                                    COALESCE(gross_outlays_undelivered_fyb, 0)) +
                                                   (COALESCE(gross_outlays_delivered_or_cpe, 0) -
                                                    COALESCE(gross_outlays_delivered_or_fyb, 0))) AS "difference",
    display_tas AS "uniqueid_TAS",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(gross_outlay_amount_by_awa_cpe, 0) <>
        (COALESCE(gross_outlays_undelivered_cpe, 0) - COALESCE(gross_outlays_undelivered_fyb, 0)) +
        (COALESCE(gross_outlays_delivered_or_cpe, 0) - COALESCE(gross_outlays_delivered_or_fyb, 0));
