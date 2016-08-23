SELECT approp.allocation_transfer_agency,
	approp.agency_identifier,
	approp.beginning_period_of_availa,
	approp.ending_period_of_availabil,
	approp.availability_type_code,
	approp.main_account_code,
	approp.sub_account_code
FROM appropriation AS approp
	JOIN submission AS sub
	    ON approp.submission_id = sub.submission_id
WHERE approp.submission_id = {0}
	AND NOT EXISTS (
		SELECT 1
		FROM sf_133 AS sf
        WHERE approp.tas IS NOT DISTINCT FROM sf.tas
            AND sf.period = sub.reporting_fiscal_period
	        AND sf.fiscal_year = sub.reporting_fiscal_year
	        AND sf.agency_identifier = sub.cgac_code
	);