SELECT
    op.row_number,
    op.object_class,
    op.by_direct_reimbursable_fun
FROM object_class_program_activity op
WHERE op.submission_id = {}
AND LENGTH(op.object_class) = 4
AND ((LEFT(op.object_class,1) = '1'
AND LOWER(op.by_direct_reimbursable_fun) IS DISTINCT FROM 'd')
OR (LEFT(op.object_class,1) = '2' AND LOWER(op.by_direct_reimbursable_fun) IS DISTINCT FROM 'r'))
