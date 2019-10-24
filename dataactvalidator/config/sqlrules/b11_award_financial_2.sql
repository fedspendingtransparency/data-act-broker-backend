-- Must be valid 3-digit object class as defined in OMB Circular A-11 Section 83.6, or a 4-digit code which includes a
-- 1-digit prefix that distinguishes direct, reimbursable, and allocation obligations. Do not include decimal points
-- when reporting in the Schema. Object Class Codes of 000 will prompt a warning.
SELECT
    row_number,
    object_class,
    tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM award_financial
WHERE submission_id = {0}
    AND object_class IN ('0000', '000', '00', '0');

