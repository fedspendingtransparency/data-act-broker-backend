SELECT op.row_number, op.bydirectreimbursablefundingsource
FROM object_class_program_activity AS op
WHERE op.submission_id = {}
    AND COALESCE(lower(op.bydirectreimbursablefundingsource),'') NOT IN ('', 'r', 'd')