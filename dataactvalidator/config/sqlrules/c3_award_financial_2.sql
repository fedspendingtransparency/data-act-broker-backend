-- ObligationsUndeliveredOrdersUnpaidTotal (CPE) = USSGL(4801 + 4831 + 4881) for the same TAS/DEFC/PYA combination.
-- This applies to the award level.
SELECT
    row_number,
    obligations_undelivered_or_cpe,
    ussgl480100_undelivered_or_cpe,
    ussgl483100_undelivered_or_cpe,
    ussgl488100_upward_adjustm_cpe,
    COALESCE(obligations_undelivered_or_cpe, 0) - (COALESCE(ussgl480100_undelivered_or_cpe, 0) +
                                                   COALESCE(ussgl483100_undelivered_or_cpe, 0) +
                                                   COALESCE(ussgl488100_upward_adjustm_cpe, 0)) AS "difference",
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    prior_year_adjustment AS "uniqueid_PriorYearAdjustment",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(obligations_undelivered_or_cpe, 0) <>
        COALESCE(ussgl480100_undelivered_or_cpe, 0) +
        COALESCE(ussgl483100_undelivered_or_cpe, 0) +
        COALESCE(ussgl488100_upward_adjustm_cpe, 0);
