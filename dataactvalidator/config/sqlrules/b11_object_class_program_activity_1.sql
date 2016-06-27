SELECT op.row_number, op.objectclass
FROM object_class_program_activity AS op
WHERE op.submission_id = {} AND op.objectclass NOT IN (SELECT object_class_code
                                                        FROM object_class)