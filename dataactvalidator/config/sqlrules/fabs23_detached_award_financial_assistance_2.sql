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
            ON frec.frec_id = sta.frec_id),
detached_award_financial_assistance_23_2_{0} AS
	(SELECT row_number,
		awarding_sub_tier_agency_c,
		awarding_office_code
	FROM detached_award_financial_assistance
	WHERE submission_id = {0}
		AND COALESCE(awarding_sub_tier_agency_c, '') <> ''
		AND COALESCE(awarding_office_code, '') <> ''),
-- make a list of all VALID pairings of office and sub tier codes with the same agency code from the submission
sub_tier_offices_{0} AS
	(SELECT stac.sub_tier_code AS sub_tier_code,
		office.office_code AS office_code
	FROM sub_tier_agency_codes_{0} AS stac
	JOIN office
		ON stac.agency_code = office.agency_code
	WHERE EXISTS (
	        SELECT 1
		    FROM detached_award_financial_assistance_23_2_{0} AS dafa
		    WHERE dafa.awarding_sub_tier_agency_c = stac.sub_tier_code
			    AND UPPER(dafa.awarding_office_code) = UPPER(office.office_code)
	))
SELECT
    row_number,
    awarding_sub_tier_agency_c,
    awarding_office_code
FROM detached_award_financial_assistance_23_2_{0} AS dafa
WHERE NOT EXISTS (
        -- Find all awarding sub tier agency and office codes that are not part of the valid pairings list
        SELECT 1
	    FROM sub_tier_offices_{0} AS sto
	    WHERE sto.sub_tier_code = dafa.awarding_sub_tier_agency_c
		    AND UPPER(sto.office_code) = UPPER(dafa.awarding_office_code)
	);
