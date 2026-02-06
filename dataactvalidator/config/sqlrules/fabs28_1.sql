-- FaceValueOfDirectLoanOrLoanGuarantee is required for loans (i.e., when AssistanceType = 07, 08, F003, or F004).
SELECT
    row_number,
    assistance_type,
    face_value_loan_guarantee,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND assistance_type IN ('07', '08', 'F003', 'F004')
    AND face_value_loan_guarantee IS NULL
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
