-- Record type is required and cannot be blank. It must be 1, 2, or 3.
SELECT
    row_number,
    record_type,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(record_type, -1) NOT IN (1, 2, 3)
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
