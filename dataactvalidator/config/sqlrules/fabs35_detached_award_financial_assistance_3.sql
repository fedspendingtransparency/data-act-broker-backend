-- LegalEntityZIP5 + LegalEntityZIPLast4 is not a valid 9 digit zip.
SELECT
    dafa.row_number,
    dafa.legal_entity_zip5,
    dafa.legal_entity_zip_last4
FROM detached_award_financial_assistance as dafa
WHERE submission_id = {0}
    AND COALESCE(dafa.legal_entity_zip5, '') != ''
    AND COALESCE(dafa.legal_entity_zip_last4, '') != ''
    AND NOT EXISTS
        (SELECT *
		FROM zips AS z
		WHERE dafa.legal_entity_zip5 = z.zip5
		    AND dafa.legal_entity_zip_last4 = z.zip_last4)