-- For each unique FAIN in File C (award financial), the sum of each TransactionObligatedAmount should match (but with
-- opposite signs) the sum of the FederalActionObligation or OriginalLoanSubsidyCost amounts reported in
-- D2 (award financial assistance). This rule does not apply for rows where the AllocationTransferAgencyIdentifier (ATA)
-- field is populated and is different from the AgencyIdentifier (AID) field, it only applies when the ATA and AID are
-- the same, or for the rows without an ATA. Note that this only compares award identifiers when the
-- TransactionObligatedAmount is not null.
-- gather the grouped sum for award financial data
WITH award_financial_c23_3_{0} AS
    (SELECT UPPER(fain) AS fain,
        SUM(transaction_obligated_amou) AS sum_ob_amount
    FROM award_financial
    WHERE submission_id = {0}
        AND transaction_obligated_amou IS NOT NULL
        AND (COALESCE(allocation_transfer_agency, '') = ''
            OR allocation_transfer_agency = agency_identifier)
    GROUP BY UPPER(fain)),
-- gather the grouped sum for award financial assistance data
award_financial_assistance_c23_3_{0} AS
    (SELECT UPPER(fain) AS fain,
        COALESCE(SUM(CASE WHEN COALESCE(assistance_type, '') IN ('07', '08')
                        THEN original_loan_subsidy_cost::NUMERIC
                        ELSE 0
                    END), 0) AS sum_orig_loan_sub_amount,
        COALESCE(SUM(CASE WHEN COALESCE(assistance_type, '') NOT IN ('07', '08')
                        THEN federal_action_obligation
                        ELSE 0
                    END), 0) AS sum_fed_act_ob_amount
    FROM award_financial_assistance
    WHERE submission_id = {0}
        AND record_type IN ('2', '3')
    GROUP BY UPPER(fain))
SELECT
    NULL AS "source_row_number",
    af.fain AS "source_value_fain",
    af.sum_ob_amount AS "source_value_transaction_obligated_amou_sum",
    afa.sum_fed_act_ob_amount AS "target_value_federal_action_obligation_sum",
    afa.sum_orig_loan_sub_amount AS "target_value_original_loan_subsidy_cost_sum",
    af.sum_ob_amount - (-1 * afa.sum_fed_act_ob_amount - afa.sum_orig_loan_sub_amount) AS "difference",
    af.fain AS "uniqueid_FAIN"
FROM award_financial_c23_3_{0} AS af
JOIN award_financial_assistance_c23_3_{0} AS afa
    ON af.fain = afa.fain
WHERE af.sum_ob_amount <> -1 * afa.sum_fed_act_ob_amount - afa.sum_orig_loan_sub_amount;
