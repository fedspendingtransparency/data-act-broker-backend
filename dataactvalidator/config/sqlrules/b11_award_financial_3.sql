SELECT
    af.row_number,
    af.object_class
FROM award_financial AS af
WHERE af.submission_id = {}
AND (af.object_class = '000' OR af.object_class = '0')
