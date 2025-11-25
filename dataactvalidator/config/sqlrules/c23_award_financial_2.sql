-- For each unique combination of PIID/ParentAwardId in File C (award financial), the sum of each
-- TransactionObligatedAmount should match (but with opposite signs) the sum of the FederalActionObligation in
-- D1 (award procurement) amounts reported in D1. This rule does not apply for rows where the
-- AllocationTransferAgencyIdentifier (ATA) field is populated and is different from the AgencyIdentifier (AID) field,
-- it only applies when the ATA and AID are the same, or for the rows without an ATA. Note that this only compares
-- award identifiers when the TransactionObligatedAmount is not null.
-- gather the grouped sum for award financial data
WITH award_financial_c23_2_{0} AS
    (SELECT UPPER(piid) AS piid,
    UPPER(parent_award_id) AS parent_award_id,
        SUM(transaction_obligated_amou) AS sum_ob_amount
    FROM award_financial
    WHERE submission_id = {0}
        AND transaction_obligated_amou IS NOT NULL
        AND (COALESCE(allocation_transfer_agency, '') = ''
            OR allocation_transfer_agency = agency_identifier)
    GROUP BY UPPER(parent_award_id),
        UPPER(piid)),
-- gather the grouped sum for award procurement data
award_procurement_c23_2_{0} AS
    (SELECT UPPER(piid) AS piid,
    UPPER(parent_award_id) AS parent_award_id,
        COALESCE(SUM(federal_action_obligation), 0) AS sum_fed_amount
    FROM award_procurement
    WHERE submission_id = {0}
    GROUP BY UPPER(parent_award_id),
        UPPER(piid))
SELECT
    NULL AS "source_row_number",
    af.piid AS "source_value_piid",
    af.parent_award_id AS "source_value_parent_award_id",
    af.sum_ob_amount AS "source_value_transaction_obligated_amou_sum",
    ap.sum_fed_amount AS "target_value_federal_action_obligation_sum",
    af.sum_ob_amount - (-1 * ap.sum_fed_amount) AS "difference",
    af.piid AS "uniqueid_PIID",
    af.parent_award_id AS "uniqueid_ParentAwardId"
FROM award_financial_c23_2_{0} AS af
JOIN award_procurement_c23_2_{0} AS ap
    ON af.parent_award_id = ap.parent_award_id
    AND af.piid = ap.piid
WHERE af.sum_ob_amount <> -1 * ap.sum_fed_amount;
