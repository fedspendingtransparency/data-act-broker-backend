-- Verify that all of the applicable GTASes have an associated entry in the
-- submission (file A).
SELECT DISTINCT NULL as row_number,
    sf.allocation_transfer_agency,
	sf.agency_identifier,
	sf.beginning_period_of_availa,
	sf.ending_period_of_availabil,
	sf.availability_type_code,
	sf.main_account_code,
	sf.sub_account_code
FROM sf_133 AS sf
	JOIN submission AS sub
		ON sf.period = sub.reporting_fiscal_period
			AND sf.fiscal_year = sub.reporting_fiscal_year
			AND (
			    (sf.agency_identifier = sub.cgac_code AND sf.allocation_transfer_agency is null)
			    OR (sf.allocation_transfer_agency = sub.cgac_code)
			)
        LEFT JOIN tas_lookup ON (tas_lookup.tas_id = sf.tas_id)
WHERE sub.submission_id = {0}
	AND NOT EXISTS (
		SELECT 1
		FROM appropriation AS approp
		WHERE sf.tas IS NOT DISTINCT FROM approp.tas
			AND approp.submission_id = {0}
	)
        AND tas_lookup.financial_indicator2 IS DISTINCT FROM 'F'
;
