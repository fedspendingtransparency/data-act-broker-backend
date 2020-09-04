-- If the DisasterEmergencyFundCode element has a valid COVID-19 related code and TOA is blank, then
-- GrossOutlayByAward_CPE cannot be blank.
SELECT
    row_number,
    disaster_emergency_fund_code,
    transaction_obligated_amou,
    gross_outlay_amount_by_awa_cpe,
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode"
FROM award_financial
WHERE submission_id = {0}
    AND EXISTS (
        SELECT 1
        FROM defc
        WHERE defc.code = UPPER(disaster_emergency_fund_code) AND
            defc.group = 'covid_19'
    )
    AND COALESCE(transaction_obligated_amou, 0) = 0
    AND gross_outlay_amount_by_awa_cpe IS NULL;
