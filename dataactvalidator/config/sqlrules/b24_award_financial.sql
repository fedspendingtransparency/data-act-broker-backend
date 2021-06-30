-- DEFC values must be A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, or T
-- (plus future codes as determined by OMB). DEFC cannot be blank.
SELECT
    row_number,
    disaster_emergency_fund_code,
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode"
FROM award_financial
WHERE submission_id = {0}
    AND NOT EXISTS (
        SELECT 1
        FROM defc
        WHERE defc.code = UPPER(disaster_emergency_fund_code)
    );
