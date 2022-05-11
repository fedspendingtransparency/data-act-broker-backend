-- AwardDescription is required for all submissions except delete records, but was not provided in this row.
SELECT
    row_number,
    award_description,
    correction_delete_indicatr,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(award_description, '') = ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
