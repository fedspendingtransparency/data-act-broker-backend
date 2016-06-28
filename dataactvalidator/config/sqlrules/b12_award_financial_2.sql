SELECT
    af.row_number,
    af.by_direct_reimbursable_fun
FROM award_financial AS af
WHERE af.submission_id = {}
AND COALESCE(LOWER(af.by_direct_reimbursable_fun),'') NOT IN ('', 'r', 'd')
