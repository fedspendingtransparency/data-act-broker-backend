SELECT
    op.row_number,
    op.availability_type_code,
    op.beginning_period_of_availa,
    op.ending_period_of_availabil
FROM object_class_program_activity AS op
WHERE op.submission_id = {}
    AND (op.availability_type_code = 'x' OR op.availability_type_code = 'X')
    AND (op.beginning_period_of_availa IS NOT NULL OR op.ending_period_of_availabil IS NOT NULL);