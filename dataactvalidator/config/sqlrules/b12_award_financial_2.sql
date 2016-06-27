SELECT af.row_number, af.bydirectreimbursablefundingsource
FROM award_financial AS af
WHERE af.submission_id = {}
    AND COALESCE(lower(af.bydirectreimbursablefundingsource),'') NOT IN ('', 'r', 'd')