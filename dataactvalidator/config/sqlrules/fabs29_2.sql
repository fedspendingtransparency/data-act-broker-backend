-- OriginalLoanSubsidyCost must be blank for non-loans (i.e., when AssistanceType is not 07, 08, F003, or F004).
SELECT
    row_number,
    assistance_type,
    original_loan_subsidy_cost,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND assistance_type NOT IN ('07', '08', 'F003', 'F004')
    AND COALESCE(original_loan_subsidy_cost, 0) <> 0
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
