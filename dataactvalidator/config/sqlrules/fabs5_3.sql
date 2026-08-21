-- When provided, AwardAmountBasisCode must contain one of the values between "A01" and "A04"
SELECT
    row_number,
    award_amount_basis_code,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND UPPER(COALESCE(award_amount_basis_code, '')) NOT IN ('', 'A01', 'A02', 'A03', 'A04')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
