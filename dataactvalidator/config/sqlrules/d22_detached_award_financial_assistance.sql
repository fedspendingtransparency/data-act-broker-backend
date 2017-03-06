-- AwardingAgencyCode is optional. When provided must be a valid 3-digit CGAC agency code.
-- If the AwardingAgencyCode is blank, it is auto-populated from the AwardingSubTierAgencyCode.
SELECT
    dafa.row_number,
    dafa.awarding_agency_code,
    dafa.awarding_sub_tier_agency_c
FROM detached_award_financial_assistance as dafa
WHERE dafa.submission_id = {0}
    AND (dafa.awarding_agency_code != ''
         AND NOT EXISTS (
            SELECT cgac.cgac_code
            FROM cgac AS cgac
            WHERE cgac.cgac_code = dafa.awarding_agency_code))
    OR (dafa.awarding_agency_code =''
        AND (dafa.awarding_agency_code != dafa.awarding_sub_tier_agency_c))