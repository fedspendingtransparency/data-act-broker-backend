-- FundingOpportunityNumber must be blank for non-grants/non-cooperative agreements
-- (AssistanceType = 06, 07, 08, 09, 10, or 11).

SELECT
    row_number,
    funding_opportunity_number,
    assistance_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(funding_opportunity_number, '') <> ''
    AND COALESCE(assistance_type, '') IN ('06', '07', '08', '09', '10', '11')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
