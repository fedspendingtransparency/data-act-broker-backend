-- Must be valid 3-digit object class as defined in OMB Circular A-11 Section 83.6, or a 4-digit code which includes a
-- 1-digit prefix that distinguishes direct, reimbursable, and allocation obligations. Do not include decimal points
-- when reporting in the Schema.
SELECT
    row_number,
    object_class,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM award_financial
WHERE submission_id = {0}
    AND object_class NOT IN ('0000', '000', '00', '0')
    AND object_class NOT IN (SELECT object_class_code
                             FROM object_class);
