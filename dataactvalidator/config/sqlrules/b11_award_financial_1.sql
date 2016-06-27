SELECT af.row_number, af.objectclass
FROM award_financial AS af
WHERE af.submission_id = {} AND af.objectclass NOT IN (SELECT object_class_code
                                                        FROM object_class)