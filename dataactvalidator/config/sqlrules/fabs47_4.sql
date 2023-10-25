-- When provided, FundingOpportunityNumber should match a FundingOpportunityNumber within an existing notice of funding
-- opportunity on Grants.gov. Agencies may provide the value "Not Applicable"

SELECT
    row_number,
    funding_opportunity_number,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND NOT EXISTS (
        SELECT 1
        FROM funding_opportunity AS fo
        WHERE UPPER(fo.funding_opportunity_number) = UPPER(fabs.funding_opportunity_number)
    )
    AND UPPER(funding_opportunity_number) <> 'NOT APPLICABLE'
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
