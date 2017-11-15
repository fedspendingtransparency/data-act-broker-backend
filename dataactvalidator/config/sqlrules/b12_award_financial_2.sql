-- Valid reimbursable flag indicator values are "r" and "d"
SELECT
    af.row_number,
    af.by_direct_reimbursable_fun
FROM award_financial AS af
WHERE af.submission_id = {0}
    AND COALESCE(UPPER(af.by_direct_reimbursable_fun), '') NOT IN ('', 'R', 'D');
