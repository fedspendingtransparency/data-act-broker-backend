-- Beginning in FY25, if the row is a non-TOA (balance) row, then
-- USSGL487200_DownwardAdjustmentsOfPriorYearPrepaidAdvancedUndeliveredOrdersObligationsRefundsCollected_CPE and
-- USSGL497200_DownwardAdjustmentsOfPriorYearPaidDeliveredOrdersObligationsRefundsCollected_CPE cannot be blank.
SELECT
    row_number,
    ussgl487200_downward_adjus_cpe,
    ussgl497200_downward_adjus_cpe,
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial AS af
JOIN submission AS sub
    ON sub.submission_id = af.submission_id
WHERE af.submission_id = {0}
    AND (ussgl487200_downward_adjus_cpe IS NULL
        OR ussgl497200_downward_adjus_cpe IS NULL)
    AND transaction_obligated_amou IS NULL
    AND sub.reporting_fiscal_year >= 2025;
