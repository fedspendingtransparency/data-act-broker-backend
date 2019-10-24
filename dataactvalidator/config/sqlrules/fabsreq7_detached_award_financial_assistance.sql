-- AssistanceType is required for all submissions except delete records, but was not provided in this row.
SELECT
    row_number,
    assistance_type,
    correction_delete_indicatr,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(assistance_type, '') = ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
