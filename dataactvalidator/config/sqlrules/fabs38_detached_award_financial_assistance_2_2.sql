-- For new modifications to existing awards, we derive FundingOfficeCode from the original transaction establishing the
-- award, when it is provided there and not provided in the new modification. For this row, we are declining to derive
-- the FundingOfficeCode from the original transaction establishing the award because the FundingOfficeCode is not
-- present in the Federal Hierarchy. If the code is accurate, please update the Federal Hierarchy to include it. If it
-- is not accurate, please correct the original award transaction to reference a valid Financial Assistance AAC/office
-- code in the hierarchy. This validation rule does not apply to delete records (CorrectionDeleteIndicator = D).
WITH detached_award_financial_assistance_38_2_2_{0} AS
    (SELECT unique_award_key,
    	row_number,
    	funding_office_code,
    	award_modification_amendme
    FROM detached_award_financial_assistance AS dafa
    WHERE dafa.submission_id = {0}
        AND UPPER(COALESCE(dafa.correction_delete_indicatr, '')) <> 'D'
        AND COALESCE(funding_office_code, '') = ''),
min_dates_{0} AS
    (SELECT unique_award_key,
        MIN(cast_as_date(action_date)) AS min_date
    FROM published_award_financial_assistance AS pafa
    WHERE is_active IS TRUE
        AND EXISTS (
            SELECT 1
            FROM detached_award_financial_assistance_38_2_2_{0} AS dafa
            WHERE pafa.unique_award_key = dafa.unique_award_key)
    GROUP BY unique_award_key),
funding_codes_{0} AS
	(SELECT pafa.unique_award_key,
		pafa.funding_office_code,
		pafa.award_modification_amendme
	FROM published_award_financial_assistance AS pafa
	JOIN min_dates_{0} AS md
		ON md.unique_award_key = pafa.unique_award_key
			AND md.min_date = cast_as_date(pafa.action_date)
	WHERE COALESCE(pafa.funding_office_code, '') <> '')
SELECT
    row_number,
    funding_office_code
FROM detached_award_financial_assistance_38_2_2_{0} AS dafa
WHERE EXISTS (
	SELECT 1
	FROM funding_codes_{0} AS fc
	WHERE dafa.unique_award_key = fc.unique_award_key
		AND dafa.award_modification_amendme != fc.award_modification_amendme
		AND NOT EXISTS (
			SELECT 1
			FROM office
			WHERE UPPER(fc.funding_office_code) = UPPER(office.office_code)
			    AND office.funding_office IS TRUE));
