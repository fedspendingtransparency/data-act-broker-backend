-- DEFC values must be A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T or 9
-- (plus future codes as determined by OMB). DEFC cannot be blank.
SELECT
    row_number,
    disaster_emergency_fund_code,
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND UPPER(disaster_emergency_fund_code) NOT IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                                                    'N', 'O', 'P', 'Q', 'R', 'S', 'T', '9');
