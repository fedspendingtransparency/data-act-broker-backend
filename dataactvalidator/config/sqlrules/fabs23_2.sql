-- If both are submitted, AwardingSubTierAgencyCode and AwardingOfficeCode must belong to the same AwardingAgencyCode
-- (per the Federal Hierarchy) at the time the award was signed (per the Action Date).
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
fabs23_2_{0} AS
	(SELECT row_number,
		awarding_sub_tier_agency_c,
		awarding_office_code,
		correction_delete_indicatr,
		afa_generated_unique,
		action_date
	FROM fabs
	WHERE submission_id = {0}
		AND COALESCE(awarding_sub_tier_agency_c, '') <> ''
		AND COALESCE(awarding_office_code, '') <> ''),
-- make a list of all VALID pairings of office and sub tier codes with the same agency code from the submission
sub_tier_offices_{0} AS
	(SELECT stac.sub_tier_code AS sub_tier_code,
		office.office_code AS office_code,
		office.effective_start_date AS effective_start_date,
		office.effective_end_date AS effective_end_date
	FROM sub_tier_agency_codes_{0} AS stac
	JOIN office
		ON stac.agency_code = office.agency_code
	WHERE EXISTS (
	        SELECT 1
		    FROM fabs23_2_{0} AS fabs
		    WHERE UPPER(fabs.awarding_sub_tier_agency_c) = UPPER(stac.sub_tier_code)
			    AND UPPER(fabs.awarding_office_code) = UPPER(office.office_code)
	))
SELECT
    row_number,
    awarding_sub_tier_agency_c,
    awarding_office_code,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs23_2_{0} AS fabs
WHERE NOT EXISTS (
        -- Find all awarding sub tier agency and office codes that are not part of the valid pairings list
        SELECT 1
	    FROM sub_tier_offices_{0} AS sto
	    WHERE UPPER(sto.sub_tier_code) = UPPER(fabs.awarding_sub_tier_agency_c)
		    AND UPPER(sto.office_code) = UPPER(fabs.awarding_office_code)
		    AND sto.effective_start_date <= cast_as_date(fabs.action_date)
		    AND COALESCE(sto.effective_end_date, NOW() + INTERVAL '1 year') > cast_as_date(fabs.action_date)
	)
	AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
