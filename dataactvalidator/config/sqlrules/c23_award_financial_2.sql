-- For each unique combination of PIID/ParentAwardId in File C (award financial), the sum of each
-- TransactionObligatedAmount should match (but with opposite signs) the sum of the FederalActionObligation in
-- D1 (award procurement) amounts reported in D1. This rule applies when the ATA and AID are the same, when the ATA is
-- the submitting agency, or when the ATA is blank. Note that this only compares award identifiers when the
-- TransactionObligatedAmount is not null.

-- All ATAs are always 3 characters so any FRECs in the associated codes of the exception list from file A are
-- extraneous
WITH cgac_exceptions AS (
	SELECT *
	FROM (VALUES
	    ('097', ARRAY ['017', '021', '057', '097']),
	    ('020', ARRAY ['020', '580', '373']),
	    ('077', ARRAY ['077', '071']),
	    ('089', ARRAY ['089', '486']),
	    ('1100', ARRAY ['256'])
	) AS t (agency_code, associated_codes)
),
-- get associations between FRECs and CGACs
frec_list AS (
    SELECT frec.frec_code,
           cgac.cgac_code
    FROM frec
    JOIN cgac
        ON cgac.cgac_id=frec.cgac_id
),
-- Figure out which cgac list is associated with this submission (exceptions first, FREC associations second, then CGAC
-- from the submission itself)
sub_cgac_{0} AS (
    SELECT
        submission_id,
        CASE WHEN ce.associated_codes IS NOT NULL THEN ce.associated_codes
        WHEN COALESCE(sub.frec_code, '') <> '' THEN ARRAY [fl.cgac_code]
        ELSE ARRAY [sub.cgac_code]
        END AS cgac_list
    FROM submission AS sub
    LEFT JOIN cgac_exceptions AS ce
        ON COALESCE(sub.cgac_code, sub.frec_code) = ce.agency_code
    LEFT JOIN frec_list As fl
        ON sub.frec_code = fl.frec_code
    WHERE sub.submission_id = {0}
),
-- gather the grouped sum for award financial data
award_financial_c23_2_{0} AS
    (SELECT UPPER(piid) AS piid,
    UPPER(parent_award_id) AS parent_award_id,
        SUM(transaction_obligated_amou) AS sum_ob_amount
    FROM award_financial AS af
    JOIN sub_cgac_{0} AS sc
        ON sc.submission_id = af.submission_id
    WHERE af.submission_id = {0}
        AND transaction_obligated_amou IS NOT NULL
        AND (COALESCE(allocation_transfer_agency, '') = ''
            OR allocation_transfer_agency = agency_identifier
            OR allocation_transfer_agency = ANY(sc.cgac_list))
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
