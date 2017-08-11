-- FaceValueLoanGuarantee must be blank for non-loans (i.e., when AssistanceType is not 07 or 08).
SELECT
    row_number,
    assistance_type,
    face_value_loan_guarantee
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND assistance_type != '07'
    AND assistance_type != '08'
    AND face_value_loan_guarantee IS NOT NULL