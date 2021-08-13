-- Prior to FY22, if the DisasterEmergencyFundCode element has a valid COVID-19 related code and the row is a balance
-- row, then GrossOutlayAmountByAward_CPE cannot be blank. Beginning in FY22, if the row is a balance row, then
-- GrossOutlayAmountByAward_CPE cannot be blank.
SELECT
    row_number,
    disaster_emergency_fund_code,
    transaction_obligated_amou,
    gross_outlay_amount_by_awa_cpe,
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode"
FROM award_financial AS af
JOIN submission AS sub
    ON sub.submission_id = af.submission_id
WHERE af.submission_id = {0}
    AND transaction_obligated_amou IS NULL
    AND gross_outlay_amount_by_awa_cpe IS NULL
    AND (sub.reporting_fiscal_year >= 2022
        OR (sub.reporting_fiscal_year < 2022
            AND EXISTS (
                SELECT 1
                FROM defc
                WHERE defc.code = UPPER(disaster_emergency_fund_code) AND
                    defc.group = 'covid_19'
            )
        )
    );
