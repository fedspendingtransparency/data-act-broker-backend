-- When provided, CorrectionDeleteIndicator must contain one of the following values: ""C"", ""D"".
SELECT
    row_number,
    correction_delete_indicatr,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(UPPER(correction_delete_indicatr), '') NOT IN ('', 'C', 'D');
