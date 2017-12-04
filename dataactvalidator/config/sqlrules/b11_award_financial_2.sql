-- Must be valid 3-digit object class as defined in OMB Circular A-11 Section 83.6, or a 4-digit code which includes a
-- 1-digit prefix that distinguishes direct, reimbursable, and allocation obligations. Do not include decimal points
-- when reporting in the Schema. Object Class Codes of 000 will prompt a warning.
SELECT
    af.row_number,
    af.object_class
FROM award_financial AS af
WHERE af.submission_id = {0}
    AND af.object_class IN ('0000', '000', '00', '0');

