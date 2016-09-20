SELECT
    op.row_number,
    op.object_class,
    op.by_direct_reimbursable_fun
FROM object_class_program_activity AS op
WHERE op.submission_id = {} AND len(op.object_class) = 4 AND NOT COALESCE(op.by_direct_reimbursable_fun,'') = ''
AND NOT (SUBSTRING(op.object_class,1,1) = '1'
AND op.by_direct_reimbursable_fun = 'D') AND NOT (SUBSTRING(op.object_class,1,1) = '2'
AND op.by_direct_reimbursable_fun = 'R')