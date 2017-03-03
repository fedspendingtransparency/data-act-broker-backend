-- AwardingSubTierAgencyCode is required for all submissions and cannot be blank.
-- AwardingSubTierAgencyCode must contain a valid four character numeric code.
SELECT
    row_number,
    awarding_sub_tier_agency_c
FROM detached_award_financial_assistance as dafa
WHERE submission_id = {0}
    AND ((awarding_sub_tier_agency_c IS NULL) OR (awarding_sub_tier_agency_c =''))
    OR (awarding_sub_tier_agency_c != ''
        AND NOT EXISTS (
            SELECT sub_tier_agency_code
            FROM sub_tier_agency
            WHERE sub_tier_agency_code = dafa.awarding_sub_tier_agency_c)
    )