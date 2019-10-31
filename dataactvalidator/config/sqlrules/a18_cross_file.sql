-- GrossOutlayAmountByTAS_CPE (file A (appropriation)) = sum of all GrossOutlayAmountByProgramObjectClass (file B
-- (object class program activity)).
WITH appropriation_a18_{0} AS
    (SELECT row_number,
        allocation_transfer_agency,
        agency_identifier,
        beginning_period_of_availa,
        ending_period_of_availabil,
        availability_type_code,
        main_account_code,
        sub_account_code,
        gross_outlay_amount_by_tas_cpe,
        tas_id,
        submission_id,
        display_tas
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number AS "source_row_number",
    approp.gross_outlay_amount_by_tas_cpe AS "source_value_gross_outlay_amount_by_tas_cpe",
    SUM(op.gross_outlay_amount_by_pro_cpe) AS "target_value_gross_outlay_amount_by_pro_cpe_sum",
    approp.gross_outlay_amount_by_tas_cpe - SUM(op.gross_outlay_amount_by_pro_cpe) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation_a18_{0} AS approp
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
    approp.gross_outlay_amount_by_tas_cpe,
    approp.display_tas
HAVING approp.gross_outlay_amount_by_tas_cpe <> SUM(op.gross_outlay_amount_by_pro_cpe);
