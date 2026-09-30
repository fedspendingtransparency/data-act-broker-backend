-- ActionType must contain one of the following values: A1, A2, B1, C1, C2, C3, C4, D1, E1, EX, FX, G1.
SELECT
    row_number,
    action_type,
    record_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND UPPER(COALESCE(action_type, '')) NOT IN ('A1', 'A2', 'B1', 'C1', 'C2', 'C3', 'C4', 'D1', 'E1', 'EX', 'FX', 'G1')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
