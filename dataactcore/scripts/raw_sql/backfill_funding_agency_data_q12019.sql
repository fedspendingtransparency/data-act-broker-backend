WITH agency_list AS
    (SELECT (CASE WHEN sta.is_frec
                THEN frec.frec_code
                ELSE cgac.cgac_code
                END) AS agency_code,
        (CASE WHEN sta.is_frec
            THEN frec.agency_name
            ELSE cgac.agency_name
            END) AS agency_name,
        sta.sub_tier_agency_code AS sub_tier_code,
        sta.sub_tier_agency_name AS sub_tier_name
    FROM sub_tier_agency AS sta
        INNER JOIN cgac
            ON cgac.cgac_id = sta.cgac_id
        INNER JOIN frec
            ON frec.frec_id = sta.frec_id),
office_list AS
    (SELECT office.office_code,
        agency_list.agency_code,
        agency_list.agency_name,
        agency_list.sub_tier_code,
        agency_list.sub_tier_name
    FROM office
        INNER JOIN agency_list
            ON agency_list.sub_tier_code = office.sub_tier_code)
UPDATE published_award_financial_assistance AS pafa
SET funding_sub_tier_agency_co = office_list.sub_tier_code,
    funding_sub_tier_agency_na = office_list.sub_tier_name,
    funding_agency_code = office_list.agency_code,
    funding_agency_name = office_list.agency_name
FROM office_list
WHERE pafa.funding_office_code = office_list.office_code
    AND cast_as_date(pafa.action_date) >= '2018/10/01'
    AND pafa.funding_sub_tier_agency_co IS NULL
    AND pafa.funding_office_code IS NOT NULL;
