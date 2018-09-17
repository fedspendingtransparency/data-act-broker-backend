-- If both are submitted, AwardingSubTierAgencyCode and AwardingOfficeCode must belong to the same AwardingAgencyCode
-- (per the Federal Hierarchy).
WITH sub_tier_agency_codes_{0} AS
    (SELECT (CASE WHEN sta.is_frec
                THEN frec.frec_code
                ELSE cgac.cgac_code
                END) AS agency_code,
        sta.sub_tier_agency_code AS sub_tier_code
    FROM sub_tier_agency AS sta
        INNER JOIN cgac
            ON cgac.cgac_id = sta.cgac_id
        INNER JOIN frec
            ON frec.frec_id = sta.frec_id)
SELECT
    row_number,
    awarding_sub_tier_agency_c,
    awarding_office_code
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(awarding_sub_tier_agency_c, '') <> ''
    AND COALESCE(awarding_office_code, '') <> ''
    AND (SELECT agency_code
        FROM sub_tier_agency_codes_{0} AS stac
        WHERE stac.sub_tier_code = dafa.awarding_sub_tier_agency_c
        ) <>
        (SELECT agency_code
        FROM office
        WHERE UPPER(office.office_code) = UPPER(dafa.awarding_office_code)
        );
