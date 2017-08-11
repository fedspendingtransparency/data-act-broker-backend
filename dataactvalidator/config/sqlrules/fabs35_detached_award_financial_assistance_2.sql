-- LegalEntityZIP5 is not a valid zip code.
SELECT
    dafa.row_number,
    dafa.legal_entity_zip5
FROM detached_award_financial_assistance as dafa
WHERE dafa.submission_id = {0}
    AND COALESCE(dafa.legal_entity_zip5, '') != ''
    AND dafa.legal_entity_zip5 NOT IN (
		SELECT DISTINCT z.zip5
		FROM zips as z
	)