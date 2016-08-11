SELECT sf.line,
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
WHERE sub.submission_id = {0}
	AND NOT EXISTS (
		SELECT 1
		FROM appropriation AS approp
			JOIN submission AS sub
				ON approp.submission_id = sub.submission_id
		WHERE sf.tas IS NOT DISTINCT FROM approp.tas
			AND sub.submission_id = {0}
	);