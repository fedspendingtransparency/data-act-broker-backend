-- For new modifications to existing awards, we derive FundingOfficeCode from the original transaction establishing the
-- award, when it is provided there and not provided in the new modification. For this row, we are declining to derive
-- the FundingOfficeCode from the original transaction establishing the award because the FundingOfficeCode is either
-- not listed in the Federal Hierarchy or not specifically designated an Assistance Funding Office. Since June 2019,
-- the Federal Hierarchy has required that FundingOfficeCodes be flagged as either a Procurement Funding Office or an
-- Assistance Funding Office (or both). If the code you are providing in this row is indeed accurate, please update the
-- Federal Hierarchy to include it and flag it as an Assistance Funding Office. If it is not accurate, please correct
-- the original award transaction to reference a valid Financial Assistance funding AAC/office code in the hierarchy.
WITH fabs38_2_2_{0} AS
    (SELECT unique_award_key,
    	row_number,
    	funding_office_code,
    	award_modification_amendme,
    	afa_generated_unique
    FROM fabs
    WHERE submission_id = {0}
        AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
        AND COALESCE(funding_office_code, '') = ''),
min_dates_{0} AS
    (SELECT unique_award_key,
        MIN(cast_as_date(action_date)) AS min_date
    FROM published_fabs AS pf
    WHERE is_active IS TRUE
        AND EXISTS (
            SELECT 1
            FROM fabs38_2_2_{0} AS fabs
            WHERE pf.unique_award_key = fabs.unique_award_key)
    GROUP BY unique_award_key),
funding_codes_{0} AS
	(SELECT pf.unique_award_key,
		pf.funding_office_code,
		pf.award_modification_amendme
	FROM published_fabs AS pf
	JOIN min_dates_{0} AS md
		ON md.unique_award_key = pf.unique_award_key
			AND md.min_date = cast_as_date(pf.action_date)
	WHERE COALESCE(pf.funding_office_code, '') <> ''
	    AND pf.is_active IS TRUE)
SELECT
    row_number,
    funding_office_code,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs38_2_2_{0} AS fabs
WHERE EXISTS (
	SELECT 1
	FROM funding_codes_{0} AS fc
	WHERE fabs.unique_award_key = fc.unique_award_key
		AND COALESCE(fabs.award_modification_amendme, '') <> COALESCE(fc.award_modification_amendme, '')
		AND NOT EXISTS (
			SELECT 1
			FROM office
			WHERE UPPER(fc.funding_office_code) = UPPER(office.office_code)
			    AND office.financial_assistance_funding_office IS TRUE));
