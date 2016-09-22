SELECT
    op.row_number,
    op.object_class,
    op.by_direct_reimbursable_fun
FROM object_class_program_activity AS op
WHERE op.submission_id = {} AND length(op.object_class) = 3 AND COALESCE(op.by_direct_reimbursable_fun,'') = ''