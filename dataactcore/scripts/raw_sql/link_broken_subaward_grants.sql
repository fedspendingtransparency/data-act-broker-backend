WITH unlinked_subs AS
    (
        SELECT id,
            prime_id,
            sub_id,
            award_id
        FROM subaward
        WHERE subaward.unique_award_key IS NULL
            AND subaward.subaward_type = 'sub-grant'),
aw_pafa AS
    (SELECT DISTINCT ON (
            pafa.fain
        )
        pafa.fain AS fain,
        pafa.uri AS uri,
        pafa.award_description AS award_description,
        pafa.record_type AS record_type,
        pafa.awarding_agency_code AS awarding_agency_code,
        pafa.awarding_agency_name AS awarding_agency_name,
        pafa.awarding_office_code AS awarding_office_code,
        pafa.awarding_office_name AS awarding_office_name,
        pafa.funding_agency_code AS funding_agency_code,
        pafa.funding_agency_name AS funding_agency_name,
        pafa.funding_office_code AS funding_office_code,
        pafa.funding_office_name AS funding_office_name,
        pafa.business_types_desc AS business_types_desc,
        pafa.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        pafa.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
        pafa.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
        pafa.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
        pafa.unique_award_key AS unique_award_key
    FROM published_award_financial_assistance AS pafa
    WHERE is_active IS TRUE
        AND EXISTS (
            SELECT 1
            FROM unlinked_subs
            WHERE unlinked_subs.award_id = pafa.fain
        )
        {0}
    ORDER BY pafa.fain, pafa.action_date)
UPDATE subaward
SET
    -- File F Prime Awards
    unique_award_key = aw_pafa.unique_award_key,
    awarding_agency_code = aw_pafa.awarding_agency_code,
    awarding_agency_name = aw_pafa.awarding_agency_name,
    awarding_sub_tier_agency_n = aw_pafa.awarding_sub_tier_agency_n,
    awarding_office_code = aw_pafa.awarding_office_code,
    awarding_office_name = aw_pafa.awarding_office_name,
    funding_agency_code = aw_pafa.funding_agency_code,
    funding_agency_name = aw_pafa.funding_agency_name,
    funding_sub_tier_agency_co = aw_pafa.funding_sub_tier_agency_co,
    funding_sub_tier_agency_na = aw_pafa.funding_sub_tier_agency_na,
    funding_office_code = aw_pafa.funding_office_code,
    funding_office_name = aw_pafa.funding_office_name,
    business_types = aw_pafa.business_types_desc
FROM unlinked_subs
     JOIN aw_pafa
        ON unlinked_subs.award_id = aw_pafa.fain
WHERE subaward.id = unlinked_subs.id;
