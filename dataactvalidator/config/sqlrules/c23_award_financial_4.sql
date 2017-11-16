-- For each unique URI for financial assistance in File C (award financial), the sum of each TransactionObligatedAmount
-- submitted in the reporting period should match (in inverse) the sum of the FederalActionObligation and
-- OriginalLoanSubsidyCost amounts reported in D2 (award financial assistance) for the same timeframe, regardless of
-- modifications.
WITH award_financial_c23_4_{0} AS
    (SELECT transaction_obligated_amou,
        uri,
        allocation_transfer_agency
    FROM award_financial
    WHERE submission_id = {0}),
-- gather the grouped sum from the previous WITH (we need both so we can do the NOT EXISTS later)
award_financial_grouped_c23_4_{0} AS
    (SELECT uri,
        COALESCE(SUM(transaction_obligated_amou), 0) AS sum_ob_amount
    FROM award_financial_c23_4_{0}
    GROUP BY uri),
-- gather the grouped sum for award financial assistance data
award_financial_assistance_c23_4_{0} AS
    (SELECT uri,
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
    GROUP BY uri)
SELECT
    NULL AS row_number,
    af.uri,
    af.sum_ob_amount AS transaction_obligated_amou_sum,
    afa.sum_fed_act_ob_amount AS federal_action_obligation_sum,
    afa.sum_orig_loan_sub_amount AS original_loan_subsidy_cost_sum
FROM award_financial_grouped_c23_4_{0} AS af
JOIN award_financial_assistance_c23_4_{0} AS afa
    ON af.uri = afa.uri
WHERE af.sum_ob_amount <> -1 * afa.sum_fed_act_ob_amount - afa.sum_orig_loan_sub_amount
    AND NOT EXISTS (
        SELECT 1
        FROM award_financial_c23_4_{0} AS sub_af
        WHERE sub_af.uri = af.uri
            AND COALESCE(sub_af.allocation_transfer_agency, '') <> ''
    );
