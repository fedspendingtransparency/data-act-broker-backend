-- When AssistanceType is a Grant or Cooperative Agreement (AssistanceType = F001 or F002) with an ActionDate on or
-- after October 01, 2026, AwardAmountBasisCode and AwardRecipientBasisCode must be populated.
SELECT
    row_number,
    assistance_type,
    action_date,
    award_amount_basis_code,
    award_recipient_basis_code,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(assistance_type, '') IN ('F001', 'F002')
    AND cast_as_date(action_date) >= '2026/10/01'
    AND (COALESCE(award_amount_basis_code, '') = ''
        OR COALESCE(award_recipient_basis_code, '') = '')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
