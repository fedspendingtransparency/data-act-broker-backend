-- Valid reimbursable flag indicator values are "R" and "D"
SELECT
    op.row_number,
    op.by_direct_reimbursable_fun
FROM object_class_program_activity AS op
WHERE op.submission_id = {0}
    AND COALESCE(UPPER(op.by_direct_reimbursable_fun), '') NOT IN ('', 'R', 'D');
