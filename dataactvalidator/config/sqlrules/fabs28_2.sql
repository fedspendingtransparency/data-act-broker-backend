-- FaceValueOfDirectLoanOrLoanGuarantee must be blank or 0 for non-loans (i.e., when AssistanceType is not 07 or 08).
SELECT
    row_number,
    assistance_type,
    face_value_loan_guarantee,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND assistance_type <> '07'
    AND assistance_type <> '08'
    AND face_value_loan_guarantee IS NOT NULL
    AND face_value_loan_guarantee <> 0
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
