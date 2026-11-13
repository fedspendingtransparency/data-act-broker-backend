-- For each unique URI in File C (award financial), the sum of each TransactionObligatedAmount should match (but with
-- opposite signs) the sum of the FederalActionObligation or OriginalLoanSubsidyCost amounts reported in D1 or D2
-- (award financial assistance). This rule applies when the ATA and AID are the same, when the ATA is the submitting
-- agency, or when the ATA is blank. Note that this only compares award identifiers when the TransactionObligatedAmount
-- is not null. For DABS resubmissions, older Assistance Types will still be accepted prior to 10/01/26.

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
award_financial_c23_4_{0} AS
    (SELECT UPPER(uri) AS uri,
        SUM(transaction_obligated_amou) AS sum_ob_amount
    FROM award_financial AS af
    JOIN sub_cgac_{0} AS sc
        ON sc.submission_id = af.submission_id
    WHERE af.submission_id = {0}
        AND transaction_obligated_amou IS NOT NULL
        AND (COALESCE(allocation_transfer_agency, '') = ''
            OR allocation_transfer_agency = agency_identifier
            OR allocation_transfer_agency = ANY(sc.cgac_list))
    GROUP BY UPPER(uri)),
-- gather the grouped sum for award financial assistance data
award_financial_assistance_c23_4_{0} AS
    (SELECT UPPER(uri) AS uri,
        COALESCE(SUM(CASE WHEN UPPER(assistance_type) IN ('07', '08', 'F003', 'F004')
                        THEN original_loan_subsidy_cost::NUMERIC
                        ELSE 0
                    END), 0) AS sum_orig_loan_sub_amount,
        COALESCE(SUM(CASE WHEN UPPER(assistance_type) NOT IN ('07', '08', 'F003', 'F004')
                        THEN federal_action_obligation
                        ELSE 0
                    END), 0) AS sum_fed_act_ob_amount
    FROM award_financial_assistance
    WHERE submission_id = {0}
        AND record_type = '1'
    GROUP BY UPPER(uri))
SELECT
    NULL AS "source_row_number",
    af.uri AS "source_value_uri",
    af.sum_ob_amount AS "source_value_transaction_obligated_amou_sum",
    afa.sum_fed_act_ob_amount AS "target_value_federal_action_obligation_sum",
    afa.sum_orig_loan_sub_amount AS "target_value_original_loan_subsidy_cost_sum",
    af.sum_ob_amount - (-1 * afa.sum_fed_act_ob_amount - afa.sum_orig_loan_sub_amount) AS "difference",
    af.uri AS "uniqueid_URI"
FROM award_financial_c23_4_{0} AS af
JOIN award_financial_assistance_c23_4_{0} AS afa
    ON af.uri = afa.uri
WHERE af.sum_ob_amount <> -1 * afa.sum_fed_act_ob_amount - afa.sum_orig_loan_sub_amount;
