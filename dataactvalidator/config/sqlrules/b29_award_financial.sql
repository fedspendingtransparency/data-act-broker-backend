-- If PYA is provided in File C, it must be on all (non-TOA) balance rows. See RSS for valid domain values.
SELECT
    row_number,
    prior_year_adjustment,
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial
WHERE submission_id = {0}
    AND transaction_obligated_amou IS NULL
    AND COALESCE(prior_year_adjustment, '') = ''
    AND EXISTS (
        SELECT 1
        FROM award_financial
        WHERE submission_id = {0}
            AND COALESCE(prior_year_adjustment, '') <> ''
    );
