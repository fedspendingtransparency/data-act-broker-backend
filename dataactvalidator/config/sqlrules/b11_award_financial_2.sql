SELECT
    af.row_number,
    af.object_class,
    af.by_direct_reimbursable_fun
FROM award_financial af
WHERE af.submission_id = {}
AND LENGTH(af.object_class) = 4
AND ((LEFT(af.object_class,1) = '1' AND LOWER(af.by_direct_reimbursable_fun) IS DISTINCT FROM 'd')
OR (LEFT(af.object_class,1) = '2' AND LOWER(af.by_direct_reimbursable_fun) IS DISTINCT FROM 'r'))
