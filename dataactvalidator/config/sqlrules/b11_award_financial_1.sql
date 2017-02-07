SELECT
    af.row_number,
    af.object_class
FROM award_financial AS af
WHERE af.submission_id = {}
AND af.object_class NOT IN (SELECT object_class_code FROM object_class)
AND af.object_class NOT IN ('000', '00', '0')
