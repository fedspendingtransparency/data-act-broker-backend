SELECT
    af.row_number,
    af.object_class
FROM award_financial AS af
WHERE af.submission_id = {}
AND af.object_class IN ('0000', '000', '00', '0')
