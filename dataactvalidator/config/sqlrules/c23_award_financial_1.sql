-- For each unique PIID in File C (award financial), the sum of each TransactionObligatedAmount should match (but with
-- opposite signs) the sum of the FederalActionObligation amounts reported in D1 (award procurement). This rule does not
-- apply for rows where the AllocationTransferAgencyIdentifier (ATA) field is populated and is different from the
-- AgencyIdentifier (AID) field, it only applies when the ATA and AID are the same, or for the rows without an ATA.
-- Note that this only compares award identifiers when the TransactionObligatedAmount is not null.
WITH award_financial_c23_1_{0} AS
    (SELECT UPPER(piid) AS piid,
        SUM(transaction_obligated_amou) AS sum_ob_amount
    FROM award_financial
    WHERE submission_id = {0}
        AND COALESCE(parent_award_id, '') = ''
        AND transaction_obligated_amou IS NOT NULL
        AND (COALESCE(allocation_transfer_agency, '') = ''
            OR allocation_transfer_agency = agency_identifier)
    GROUP BY UPPER(piid)),
-- gather the grouped sum for award procurement data
award_procurement_c23_1_{0} AS
    (SELECT UPPER(piid) AS piid,
        COALESCE(SUM(federal_action_obligation), 0) AS sum_fed_amount
    FROM award_procurement
    WHERE submission_id = {0}
        AND COALESCE(parent_award_id, '') = ''
    GROUP BY UPPER(piid))
SELECT
    NULL AS "source_row_number",
    af.piid AS "source_value_piid",
    af.sum_ob_amount AS "source_value_transaction_obligated_amou_sum",
    ap.sum_fed_amount AS "target_value_federal_action_obligation_sum",
    af.sum_ob_amount - (-1 * ap.sum_fed_amount) AS "difference",
    af.piid AS "uniqueid_PIID"
FROM award_financial_c23_1_{0} AS af
JOIN award_procurement_c23_1_{0} AS ap
    ON af.piid = ap.piid
WHERE af.sum_ob_amount <> -1 * ap.sum_fed_amount;
