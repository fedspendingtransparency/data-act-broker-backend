SELECT
    op.row_number,
    op.object_class
FROM object_class_program_activity AS op
WHERE op.submission_id = {}
AND op.object_class = '000'
