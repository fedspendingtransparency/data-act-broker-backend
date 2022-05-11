-- When provided, FundingSubTierAgencyCode must be a valid 4-character sub-tier agency code from the Federal Hierarchy.
SELECT
    dafa.row_number,
    dafa.funding_sub_tier_agency_co,
    dafa.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance AS dafa
WHERE dafa.submission_id = {0}
    AND dafa.funding_sub_tier_agency_co <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM sub_tier_agency AS sta
        WHERE UPPER(sta.sub_tier_agency_code) = UPPER(dafa.funding_sub_tier_agency_co)
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
