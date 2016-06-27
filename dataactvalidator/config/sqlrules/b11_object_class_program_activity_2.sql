SELECT op.row_number, op.objectclass, op.bydirectreimbursablefundingsource
FROM object_class_program_activity op
WHERE op.submission_id = {} AND LENGTH(op.objectclass) = 4
                            AND ((LEFT(op.objectclass,1) = '1'
                                AND lower(op.bydirectreimbursablefundingsource) IS DISTINCT FROM 'd')
                                OR (LEFT(op.objectclass,1) = '2'
                                    AND lower(op.bydirectreimbursablefundingsource) IS DISTINCT FROM 'r'))