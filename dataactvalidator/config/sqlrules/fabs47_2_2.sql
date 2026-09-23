-- FundingOpportunityNumber is required for all competitive, discretionary grants and cooperative agreements
-- (AssistanceType = F001 or F002 and AwardRecipientBasisCode = R01) with an ActionDate on or after October 01, 2026.

SELECT
    row_number,
    funding_opportunity_number,
    assistance_type,
    award_recipient_basis_code,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(funding_opportunity_number, '') = ''
    AND UPPER(assistance_type) IN ('F001', 'F002')
    AND UPPER(COALESCE(award_recipient_basis_code, '')) = 'R01'
    AND cast_as_date(action_date) >= '2026/10/01'
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
