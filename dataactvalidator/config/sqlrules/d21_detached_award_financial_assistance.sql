--  FundingSubTierAgencyCode is an optional field, but when provided must be a valid 4-digit sub-tier agency code.
SELECT
    row_number,
    funding_sub_tier_agency_co
FROM detached_award_financial_assistance as dafa
WHERE submission_id = {0}
    AND funding_sub_tier_agency_co != ''
    AND NOT EXISTS (
        SELECT sub_tier_agency_code
        FROM sub_tier_agency
        WHERE sub_tier_agency_code = dafa.funding_sub_tier_agency_co
     )