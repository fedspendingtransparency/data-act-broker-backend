-- FundingOpportunityNumber is required for all grants and cooperative agreements (AssistanceType = 02, 03, 04, or 05).

SELECT
    row_number,
    funding_opportunity_number,
    assistance_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(funding_opportunity_number, '') = ''
    AND COALESCE(assistance_type, '') IN ('02', '03', '04', '05')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
