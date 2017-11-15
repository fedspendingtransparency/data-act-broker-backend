-- Valid reimbursable flag indicator values are "r" and "d"
SELECT
    op.row_number,
    op.by_direct_reimbursable_fun
FROM object_class_program_activity AS op
WHERE op.submission_id = {0}
    AND COALESCE(LOWER(op.by_direct_reimbursable_fun), '') NOT IN ('', 'r', 'd');
