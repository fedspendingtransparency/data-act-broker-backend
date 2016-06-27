SELECT af.row_number, af.objectclass, af.bydirectreimbursablefundingsource
FROM award_financial af
WHERE af.submission_id = {} AND LENGTH(af.objectclass) = 4
                            AND ((LEFT(af.objectclass,1) = '1'
                                AND lower(af.bydirectreimbursablefundingsource) IS DISTINCT FROM 'd')
                                OR (LEFT(af.objectclass,1) = '2'
                                    AND lower(af.bydirectreimbursablefundingsource) IS DISTINCT FROM 'r'))