-- ActionType field must contain A, B, C, D, or blank
SELECT
    row_number,
    action_type,
    record_type,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(COALESCE(action_type, '')) NOT IN ('', 'A', 'B', 'C', 'D')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
