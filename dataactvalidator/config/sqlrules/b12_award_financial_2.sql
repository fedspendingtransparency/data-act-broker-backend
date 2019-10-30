-- Valid reimbursable flag indicator values are "r" and "d"
SELECT
    row_number,
    by_direct_reimbursable_fun,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(UPPER(by_direct_reimbursable_fun), '') NOT IN ('', 'R', 'D');
