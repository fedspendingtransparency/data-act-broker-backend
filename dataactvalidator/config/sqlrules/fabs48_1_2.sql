-- FundingOpportunityGoalsText must be blank or "Not Applicable" for AssistanceType = F001 or F002 and
-- AwardRecipientBasisCode = R02, R03, or R04.

SELECT
    row_number,
    funding_opportunity_goals,
    assistance_type,
    award_recipient_basis_code,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND UPPER(COALESCE(funding_opportunity_goals, '')) NOT IN ('', 'NOT APPLICABLE')
    AND UPPER(COALESCE(assistance_type, '')) IN ('F001', 'F002')
    AND UPPER(COALESCE(award_recipient_basis_code, '')) IN ('R02', 'R03', 'R04')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
