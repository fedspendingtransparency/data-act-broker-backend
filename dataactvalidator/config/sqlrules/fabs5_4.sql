-- When provided, AwardRecipientBasisCode must contain one of the values between "R01" and "R04"
SELECT
    row_number,
    award_recipient_basis_code,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND UPPER(COALESCE(award_recipient_basis_code, '')) NOT IN ('', 'R01', 'R02', 'R03', 'R04')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
