-- Valid reimbursable flag indicator values are "R" and "D"
SELECT
    row_number,
    by_direct_reimbursable_fun,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND COALESCE(UPPER(by_direct_reimbursable_fun), '') NOT IN ('', 'R', 'D');
