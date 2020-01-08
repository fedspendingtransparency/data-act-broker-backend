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
aw_pafa AS
    (SELECT
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
            WHERE UPPER(TRANSLATE(unlinked_subs.award_id, '-', '')) = UPPER(TRANSLATE(pafa.fain, '-', ''))
                AND pafa.record_type != 1
                AND (unlinked_subs.awarding_sub_tier_agency_c IS NULL OR UPPER(unlinked_subs.awarding_sub_tier_agency_c)=UPPER(pafa.awarding_sub_tier_agency_c))
        )
        {0}
    ORDER BY UPPER(pafa.fain), pafa.action_date),
grouped_pafa AS (
    SELECT *
    FROM aw_pafa
    WHERE EXISTS (
        SELECT 1
        FROM (SELECT fain, awarding_sub_tier_agency_c, COUNT(1)
            FROM aw_pafa
            GROUP BY fain, awarding_sub_tier_agency_c
            HAVING COUNT(1) = 1) AS singled_pafa
        WHERE (singled_pafa.fain = aw_pafa.fain
            AND singled_pafa.awarding_sub_tier_agency_c = aw_pafa.awarding_sub_tier_agency_c)
    )
)
UPDATE subaward
SET
    -- File F Prime Awards
    unique_award_key = grouped_pafa.unique_award_key,
    awarding_agency_code = grouped_pafa.awarding_agency_code,
    awarding_agency_name = grouped_pafa.awarding_agency_name,
    awarding_sub_tier_agency_n = grouped_pafa.awarding_sub_tier_agency_n,
    awarding_office_code = grouped_pafa.awarding_office_code,
    awarding_office_name = grouped_pafa.awarding_office_name,
    funding_agency_code = grouped_pafa.funding_agency_code,
    funding_agency_name = grouped_pafa.funding_agency_name,
    funding_sub_tier_agency_co = grouped_pafa.funding_sub_tier_agency_co,
    funding_sub_tier_agency_na = grouped_pafa.funding_sub_tier_agency_na,
    funding_office_code = grouped_pafa.funding_office_code,
    funding_office_name = grouped_pafa.funding_office_name,
    business_types = grouped_pafa.business_types_desc
FROM unlinked_subs
     JOIN grouped_pafa
        ON (UPPER(TRANSLATE(unlinked_subs.award_id, '-', '')) = UPPER(TRANSLATE(grouped_pafa.fain, '-', ''))
            AND (unlinked_subs.awarding_sub_tier_agency_c IS NULL OR UPPER(unlinked_subs.awarding_sub_tier_agency_c)=UPPER(grouped_pafa.awarding_sub_tier_agency_c))
        )
WHERE subaward.id = unlinked_subs.id;
