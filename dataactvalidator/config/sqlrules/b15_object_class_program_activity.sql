-- All the Reimbursable (R) amounts reported for (4801_CPE less 4801_FYB) + (4802_CPE less 4802_FYB)
-- + 4881_CPE + 4882_CPE + (4901_CPE less 4901_FYB) + 4902_CPE + (4908_CPE less 4908_FYB) + 4981_CPE + 4982_CPE =
-- the opposite sign of GTAS SF 133 line 2104 per TAS, for the same reporting period and TAS and DEFC combination
-- where PYA = "X". If the DEFC is other than Q or QQQ, this value should be $0 since there should not be reimbursable
-- work reported for disasters or emergencies.
WITH object_class_program_activity_b15_{0} AS
    (SELECT submission_id,
        display_tas,
        UPPER(disaster_emergency_fund_code) AS disaster_emergency_fund_code,
        UPPER(prior_year_adjustment) AS prior_year_adjustment,
        SUM(ussgl480100_undelivered_or_cpe) AS ussgl480100_undelivered_or_cpe_sum,
        SUM(ussgl480100_undelivered_or_fyb) AS ussgl480100_undelivered_or_fyb_sum,
        SUM(ussgl480200_undelivered_or_cpe) AS ussgl480200_undelivered_or_cpe_sum,
        SUM(ussgl480200_undelivered_or_fyb) AS ussgl480200_undelivered_or_fyb_sum,
        SUM(ussgl488100_upward_adjustm_cpe) AS ussgl488100_upward_adjustm_cpe_sum,
        SUM(ussgl488200_upward_adjustm_cpe) AS ussgl488200_upward_adjustm_cpe_sum,
        SUM(ussgl490100_delivered_orde_cpe) AS ussgl490100_delivered_orde_cpe_sum,
        SUM(ussgl490100_delivered_orde_fyb) AS ussgl490100_delivered_orde_fyb_sum,
        SUM(ussgl490200_delivered_orde_cpe) AS ussgl490200_delivered_orde_cpe_sum,
        SUM(ussgl490800_authority_outl_cpe) AS ussgl490800_authority_outl_cpe_sum,
        SUM(ussgl490800_authority_outl_fyb) AS ussgl490800_authority_outl_fyb_sum,
        SUM(ussgl498100_upward_adjustm_cpe) AS ussgl498100_upward_adjustm_cpe_sum,
        SUM(ussgl498200_upward_adjustm_cpe) AS ussgl498200_upward_adjustm_cpe_sum,
        (
            SUM(ussgl480100_undelivered_or_cpe) - SUM(ussgl480100_undelivered_or_fyb) +
            SUM(ussgl480200_undelivered_or_cpe) - SUM(ussgl480200_undelivered_or_fyb) +
            SUM(ussgl488100_upward_adjustm_cpe) +
            SUM(ussgl488200_upward_adjustm_cpe) +
            SUM(ussgl490100_delivered_orde_cpe) - SUM(ussgl490100_delivered_orde_fyb) +
            SUM(ussgl490200_delivered_orde_cpe) +
            SUM(ussgl490800_authority_outl_cpe) - SUM(ussgl490800_authority_outl_fyb) +
            SUM(ussgl498100_upward_adjustm_cpe) +
            SUM(ussgl498200_upward_adjustm_cpe)
        ) AS sum_amount
    FROM object_class_program_activity AS op
    WHERE op.submission_id = {0}
        AND UPPER(prior_year_adjustment) = 'X'
        AND UPPER(by_direct_reimbursable_fun) = 'R'
    GROUP BY submission_id,
        display_tas,
        UPPER(disaster_emergency_fund_code),
        UPPER(prior_year_adjustment))
SELECT DISTINCT
    NULL AS row_number,
    op.display_tas AS "tas",
    UPPER(op.prior_year_adjustment) AS "prior_year_adjustment",
    ussgl480100_undelivered_or_cpe_sum,
    ussgl480100_undelivered_or_fyb_sum,
    ussgl480200_undelivered_or_cpe_sum,
    ussgl480200_undelivered_or_fyb_sum,
    ussgl488100_upward_adjustm_cpe_sum,
    ussgl488200_upward_adjustm_cpe_sum,
    ussgl490100_delivered_orde_cpe_sum,
    ussgl490100_delivered_orde_fyb_sum,
    ussgl490200_delivered_orde_cpe_sum,
    ussgl490800_authority_outl_cpe_sum,
    ussgl490800_authority_outl_fyb_sum,
    ussgl498100_upward_adjustm_cpe_sum,
    ussgl498200_upward_adjustm_cpe_sum,
    SUM(COALESCE(sf.amount, 0)) AS "expected_value_GTAS SF133 Line 2104",
    sum_amount + SUM(COALESCE(sf.amount, 0)) AS "difference",
    op.display_tas AS "uniqueid_TAS",
    UPPER(op.disaster_emergency_fund_code) AS "uniqueid_DisasterEmergencyFundCode"
FROM object_class_program_activity_b15_{0} AS op
    INNER JOIN submission AS sub
        ON op.submission_id = sub.submission_id
    LEFT OUTER JOIN sf_133 AS sf
        ON op.display_tas = sf.display_tas
        AND UPPER(op.disaster_emergency_fund_code) = UPPER(COALESCE(sf.disaster_emergency_fund_code, ''))
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
        AND sf.line = 2104
GROUP BY op.display_tas,
    ussgl480100_undelivered_or_cpe_sum,
    ussgl480100_undelivered_or_fyb_sum,
    ussgl480200_undelivered_or_cpe_sum,
    ussgl480200_undelivered_or_fyb_sum,
    ussgl488100_upward_adjustm_cpe_sum,
    ussgl488200_upward_adjustm_cpe_sum,
    ussgl490100_delivered_orde_cpe_sum,
    ussgl490100_delivered_orde_fyb_sum,
    ussgl490200_delivered_orde_cpe_sum,
    ussgl490800_authority_outl_cpe_sum,
    ussgl490800_authority_outl_fyb_sum,
    ussgl498100_upward_adjustm_cpe_sum,
    ussgl498200_upward_adjustm_cpe_sum,
    sum_amount,
    UPPER(op.disaster_emergency_fund_code),
    UPPER(op.prior_year_adjustment)
HAVING (UPPER(op.disaster_emergency_fund_code) IN ('Q', 'QQQ')
        AND sum_amount <> (-1 * SUM(COALESCE(sf.amount, 0))))
    OR (UPPER(op.disaster_emergency_fund_code) NOT IN ('Q', 'QQQ')
        AND sum_amount <> 0);
