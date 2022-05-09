WITH unlinked_subs AS
    (
        SELECT id,
            prime_id,
            sub_id,
            award_id,
            awarding_sub_tier_agency_c
        FROM subaward
        WHERE subaward.unique_award_key IS NULL
            AND subaward.subaward_type = 'sub-grant'),
aw_pf AS
    (SELECT
        pf.fain AS fain,
        pf.uri AS uri,
        pf.award_description AS award_description,
        pf.record_type AS record_type,
        pf.awarding_agency_code AS awarding_agency_code,
        pf.awarding_agency_name AS awarding_agency_name,
        pf.awarding_office_code AS awarding_office_code,
        pf.awarding_office_name AS awarding_office_name,
        pf.funding_agency_code AS funding_agency_code,
        pf.funding_agency_name AS funding_agency_name,
        pf.funding_office_code AS funding_office_code,
        pf.funding_office_name AS funding_office_name,
        pf.business_types_desc AS business_types_desc,
        pf.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        pf.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
        pf.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
        pf.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
        pf.unique_award_key AS unique_award_key
    FROM published_fabs AS pf
    WHERE is_active IS TRUE
        AND EXISTS (
            SELECT 1
            FROM unlinked_subs
            WHERE pf.record_type <> 1
                AND UPPER(TRANSLATE(unlinked_subs.award_id, '-', '')) = UPPER(TRANSLATE(pf.fain, '-', ''))
                AND UPPER(unlinked_subs.awarding_sub_tier_agency_c) IS NOT DISTINCT FROM UPPER(pf.awarding_sub_tier_agency_c)
        )
        {0}
    ORDER BY UPPER(pf.fain), pf.action_date)
UPDATE subaward
SET
    -- File F Prime Awards
    unique_award_key = aw_pf.unique_award_key,
    awarding_agency_code = aw_pf.awarding_agency_code,
    awarding_agency_name = aw_pf.awarding_agency_name,
    awarding_sub_tier_agency_n = aw_pf.awarding_sub_tier_agency_n,
    awarding_office_code = aw_pf.awarding_office_code,
    awarding_office_name = aw_pf.awarding_office_name,
    funding_agency_code = aw_pf.funding_agency_code,
    funding_agency_name = aw_pf.funding_agency_name,
    funding_sub_tier_agency_co = aw_pf.funding_sub_tier_agency_co,
    funding_sub_tier_agency_na = aw_pf.funding_sub_tier_agency_na,
    funding_office_code = aw_pf.funding_office_code,
    funding_office_name = aw_pf.funding_office_name,
    business_types = aw_pf.business_types_desc
FROM unlinked_subs
     JOIN aw_pf
        ON UPPER(TRANSLATE(unlinked_subs.award_id, '-', '')) = UPPER(TRANSLATE(aw_pf.fain, '-', ''))
        AND UPPER(unlinked_subs.awarding_sub_tier_agency_c) IS NOT DISTINCT FROM UPPER(aw_pf.awarding_sub_tier_agency_c)
WHERE subaward.id = unlinked_subs.id;
