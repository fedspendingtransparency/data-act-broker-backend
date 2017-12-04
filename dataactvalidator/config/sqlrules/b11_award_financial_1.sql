-- Must be valid 3-digit object class as defined in OMB Circular A-11 Section 83.6, or a 4-digit code which includes a
-- 1-digit prefix that distinguishes direct, reimbursable, and allocation obligations. Do not include decimal points
-- when reporting in the Schema.
SELECT
    af.row_number,
    af.object_class
FROM award_financial AS af
WHERE af.submission_id = {0}
    AND af.object_class NOT IN ('0000', '000', '00', '0')
    AND af.object_class NOT IN (SELECT object_class_code
                                FROM object_class);
