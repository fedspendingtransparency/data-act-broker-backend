-- When provided, FundingOpportunityNumber must only contain letters (a-z, lowercase or uppercase), numerals (0-10),
-- or the ‘-‘ character, to ensure consistency with Grants.gov FundingOpportunityNumber formatting requirements.

SELECT
    row_number,
    funding_opportunity_number,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND UPPER(funding_opportunity_number) !~ '^[A-Z0-9\-]*$'
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
