-- Must be a valid 3-digit object class as defined in OMB Circular A-11 Section 83.6, or a 4-digit code which includes
-- a 1-digit suffix that is typically zero. Do not include decimal points when reporting object classes
-- (e.g., "25.2" would be reported as 252 or 2520). For amounts that cannot yet be allocated to a valid object
-- class, input 000, although note that this will prompt a warning unless all obligation and outlay balances on this
-- row are $0. A fatal error will be given if Object Class is not provided.
SELECT
    row_number,
    object_class,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM award_financial
WHERE submission_id = {0}
    AND object_class IN ('0000', '000', '00', '0');

