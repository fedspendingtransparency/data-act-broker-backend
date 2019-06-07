WITH unlinked_subs AS
    (
        SELECT id,
            prime_id,
            sub_id,
            award_id,
            parent_award_id,
            awarding_sub_tier_agency_c
        FROM subaward
        WHERE subaward.unique_award_key IS NULL
            AND subaward.subaward_type = 'sub-contract'),
aw_dap AS
    (SELECT DISTINCT ON (
            dap.piid,
            dap.parent_award_id,
            dap.awarding_sub_tier_agency_c
        )
        dap.unique_award_key AS unique_award_key,
        dap.piid AS piid,
        dap.idv_type AS idv_type,
        dap.parent_award_id AS parent_award_id,
        dap.award_description as award_description,
        dap.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        dap.naics_description AS naics_description,
        dap.awarding_agency_code AS awarding_agency_code,
        dap.awarding_agency_name AS awarding_agency_name,
        dap.funding_agency_code AS funding_agency_code,
        dap.funding_agency_name AS funding_agency_name
    FROM detached_award_procurement AS dap
    WHERE EXISTS (
        SELECT 1
        FROM unlinked_subs
        WHERE unlinked_subs.award_id = dap.piid
            AND COALESCE(unlinked_subs.parent_award_id, '') = COALESCE(dap.parent_award_id, '')
            AND unlinked_subs.awarding_sub_tier_agency_c = dap.awarding_sub_tier_agency_c
        )
        {0}
    ORDER BY dap.piid, dap.parent_award_id, dap.awarding_sub_tier_agency_c, dap.action_date)
UPDATE subaward
SET
    unique_award_key = aw_dap.unique_award_key,
    awarding_agency_code = aw_dap.awarding_agency_code,
    awarding_agency_name = aw_dap.awarding_agency_name,
    funding_agency_code = aw_dap.funding_agency_code,
    funding_agency_name = aw_dap.funding_agency_name,
    award_description = aw_dap.award_description,
    naics_description = aw_dap.naics_description
FROM unlinked_subs
    JOIN aw_dap
        ON (unlinked_subs.award_id = aw_dap.piid
        AND COALESCE(unlinked_subs.parent_award_id, '') = COALESCE(aw_dap.parent_award_id, '')
        AND unlinked_subs.awarding_sub_tier_agency_c = aw_dap.awarding_sub_tier_agency_c
        )
WHERE subaward.id = unlinked_subs.id;
