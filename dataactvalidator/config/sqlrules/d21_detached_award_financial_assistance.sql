--  FundingSubTierAgencyCode is an optional field, but when provided must be a valid 4-digit sub-tier agency code.
SELECT
    dafa.row_number,
    dafa.funding_sub_tier_agency_co
FROM detached_award_financial_assistance AS dafa
WHERE dafa.submission_id = {0}
    AND dafa.funding_sub_tier_agency_co != ''
    AND NOT EXISTS (
        SELECT sta.sub_tier_agency_code
        FROM sub_tier_agency AS sta
        WHERE sta.sub_tier_agency_code = dafa.funding_sub_tier_agency_co
     )