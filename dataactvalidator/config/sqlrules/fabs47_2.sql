-- FundingOpportunityNumber is required for all grants and cooperative agreements (AssistanceType = F001 or F002).

SELECT
    row_number,
    funding_opportunity_number,
    assistance_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(funding_opportunity_number, '') = ''
    AND COALESCE(assistance_type, '') IN ('F001', 'F002')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
