-- ObligationsIncurredTotalByTAS_CPE (File A) = negative sum of ObligationsIncurredByProgramObjectClass_CPE (File B).
WITH appropriation_a19_{0} AS
	(SELECT row_number,
		allocation_transfer_agency,
		agency_identifier,
		beginning_period_of_availa,
		ending_period_of_availabil,
		availability_type_code,
		main_account_code,
		sub_account_code,
		obligations_incurred_total_cpe,
		tas_id,
		submission_id
	FROM appropriation
	WHERE submission_id = {0})
SELECT
	approp.row_number,
	approp.allocation_transfer_agency,
	approp.agency_identifier,
	approp.beginning_period_of_availa,
	approp.ending_period_of_availabil,
	approp.availability_type_code,
	approp.main_account_code,
	approp.sub_account_code,
	approp.obligations_incurred_total_cpe,
	SUM(op.obligations_incurred_by_pr_cpe) * -1 AS obligations_incurred_by_pr_cpe_sum
FROM appropriation_a19_{0} AS approp
	JOIN object_class_program_activity op
		ON approp.tas_id = op.tas_id
			AND approp.submission_id = op.submission_id
GROUP BY approp.row_number,
	approp.allocation_transfer_agency,
	approp.agency_identifier,
	approp.beginning_period_of_availa,
	approp.ending_period_of_availabil,
	approp.availability_type_code,
	approp.main_account_code,
	approp.sub_account_code,
	approp.obligations_incurred_total_cpe
HAVING approp.obligations_incurred_total_cpe <> SUM(op.obligations_incurred_by_pr_cpe) * -1;
