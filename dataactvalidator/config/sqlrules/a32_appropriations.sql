SELECT approp.row_number,
	approp.allocation_transfer_agency,
	approp.agency_identifier,
	approp.beginning_period_of_availa,
	approp.ending_period_of_availabil,
	approp.availability_type_code,
	approp.main_account_code,
	approp.sub_account_code
FROM appropriation AS approp
WHERE approp.submission_id = {0}
	AND EXISTS (
		SELECT a.row_number,
			a.tas
		FROM appropriation AS a
		WHERE a.row_number <> approp.row_number
			AND a.tas IS NOT DISTINCT FROM approp.tas
			AND a.submission_id = {0}
	);