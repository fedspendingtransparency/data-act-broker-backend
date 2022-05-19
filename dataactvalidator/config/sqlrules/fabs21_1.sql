-- When provided, FundingSubTierAgencyCode must be a valid 4-character sub-tier agency code from the Federal Hierarchy.
SELECT
    row_number,
    funding_sub_tier_agency_co,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND funding_sub_tier_agency_co <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM sub_tier_agency AS sta
        WHERE UPPER(sta.sub_tier_agency_code) = UPPER(fabs.funding_sub_tier_agency_co)
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
