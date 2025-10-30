-- GrossOutlaysUndeliveredOrdersPrepaidTotal (CPE) = USSGL(4802 + 4832 + 4882). This applies to the award level.
SELECT
    row_number,
    gross_outlays_undelivered_cpe,
    ussgl480200_undelivered_or_cpe,
    ussgl480210_rein_undel_obs_cpe,
    ussgl483200_undelivered_or_cpe,
    ussgl488200_upward_adjustm_cpe,
    COALESCE(gross_outlays_undelivered_cpe, 0) - (COALESCE(ussgl480200_undelivered_or_cpe, 0) +
                                                  COALESCE(ussgl480210_rein_undel_obs_cpe, 0) +
                                                  COALESCE(ussgl483200_undelivered_or_cpe, 0) +
                                                  COALESCE(ussgl488200_upward_adjustm_cpe, 0)) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    prior_year_adjustment AS "uniqueid_PriorYearAdjustment",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(gross_outlays_undelivered_cpe, 0) <>
        COALESCE(ussgl480200_undelivered_or_cpe, 0) +
        COALESCE(ussgl480210_rein_undel_obs_cpe, 0) +
        COALESCE(ussgl483200_undelivered_or_cpe, 0) +
        COALESCE(ussgl488200_upward_adjustm_cpe, 0);
