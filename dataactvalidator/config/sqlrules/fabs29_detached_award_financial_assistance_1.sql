-- OriginalLoanSubsidyCost is required for loans (i.e., when AssistanceType = 07 or 08).
SELECT
    row_number,
    assistance_type,
    original_loan_subsidy_cost,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (assistance_type = '07'
        OR assistance_type = '08'
    )
    AND original_loan_subsidy_cost IS NULL
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
