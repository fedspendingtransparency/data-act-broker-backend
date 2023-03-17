-- A submitted Financial Assistance award must have a (derived) AwardingAgencyCode that is consistent with the toptier
-- component of the agency selected at the outset of the FABS submission. This comparison only takes place at the
-- TopTier level, not the SubTier level.

WITH agency_list AS
    (SELECT (CASE WHEN sta.is_frec
                THEN frec.frec_code
                ELSE cgac.cgac_code
                END) AS agency_code,
        sta.is_frec,
        sta.sub_tier_agency_code
    FROM sub_tier_agency AS sta
        LEFT OUTER JOIN cgac
            ON cgac.cgac_id = sta.cgac_id
        LEFT OUTER JOIN frec
            ON frec.frec_id = sta.frec_id)
SELECT
    row_number,
    awarding_sub_tier_agency_c,
    al.agency_code AS "derived_awarding_agency_code",
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey",
    COALESCE(sub.frec_code, sub.cgac_code) AS "expected_value_Derived AwardingAgencyCode"
FROM fabs
JOIN submission AS sub
    ON sub.submission_id = fabs.submission_id
JOIN agency_list AS al
    ON al.sub_tier_agency_code = UPPER(fabs.awarding_sub_tier_agency_c)
WHERE fabs.submission_id = {0}
    AND COALESCE(sub.frec_code, sub.cgac_code) <> al.agency_code;
