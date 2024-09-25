-- All the Direct Appropriation (D) amounts reported for (4801_CPE less 4801_FYB) + (4802_CPE less 4802_FYB)
-- + 4881_CPE + 4882_CPE + (4901_CPE less 4901_FYB) + 4902_CPE + (4908_CPE less 4908_FYB) + 4981_CPE + 4982_CPE =
-- the opposite sign of GTAS SF 133 line 2004 per TAS, for the same reporting period and TAS and DEFC combination
-- where PYA = "X".
WITH object_class_program_activity_b14_{0} AS
    (SELECT submission_id,
        tas,
        display_tas,
        disaster_emergency_fund_code,
        prior_year_adjustment,
        ussgl480100_undelivered_or_cpe,
        ussgl480100_undelivered_or_fyb,
        ussgl480200_undelivered_or_cpe,
        ussgl480200_undelivered_or_fyb,
        ussgl488100_upward_adjustm_cpe,
        ussgl488200_upward_adjustm_cpe,
        ussgl490100_delivered_orde_cpe,
        ussgl490100_delivered_orde_fyb,
        ussgl490200_delivered_orde_cpe,
        ussgl490800_authority_outl_cpe,
        ussgl490800_authority_outl_fyb,
        ussgl498100_upward_adjustm_cpe,
        ussgl498200_upward_adjustm_cpe,
        by_direct_reimbursable_fun
    FROM object_class_program_activity
    WHERE submission_id = {0}
        AND UPPER(prior_year_adjustment) = 'X')
SELECT DISTINCT
    NULL AS row_number,
    op.display_tas AS "tas",
    UPPER(op.prior_year_adjustment) AS "prior_year_adjustment",
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
    sf.amount AS "expected_value_GTAS SF133 Line 2004",
    (SUM(ussgl480100_undelivered_or_cpe) - SUM(ussgl480100_undelivered_or_fyb) +
        SUM(ussgl480200_undelivered_or_cpe) - SUM(ussgl480200_undelivered_or_fyb) +
        SUM(ussgl488100_upward_adjustm_cpe) +
        SUM(ussgl488200_upward_adjustm_cpe) +
        SUM(ussgl490100_delivered_orde_cpe) - SUM(ussgl490100_delivered_orde_fyb) +
        SUM(ussgl490200_delivered_orde_cpe) +
        SUM(ussgl490800_authority_outl_cpe) - SUM(ussgl490800_authority_outl_fyb) +
        SUM(ussgl498100_upward_adjustm_cpe) +
        SUM(ussgl498200_upward_adjustm_cpe)
    ) + sf.amount AS "difference",
    op.display_tas AS "uniqueid_TAS",
    UPPER(op.disaster_emergency_fund_code) AS "uniqueid_DisasterEmergencyFundCode"
FROM object_class_program_activity_b14_{0} AS op
    INNER JOIN sf_133 AS sf
        ON op.tas = sf.tas
        AND UPPER(op.disaster_emergency_fund_code) = COALESCE(sf.disaster_emergency_fund_code, '')
    INNER JOIN submission AS sub
        ON op.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line = 2004
    AND UPPER(op.by_direct_reimbursable_fun) = 'D'
GROUP BY op.tas,
    UPPER(op.disaster_emergency_fund_code),
    sf.amount,
    op.display_tas,
    UPPER(prior_year_adjustment)
HAVING (
        SUM(ussgl480100_undelivered_or_cpe) - SUM(ussgl480100_undelivered_or_fyb) +
        SUM(ussgl480200_undelivered_or_cpe) - SUM(ussgl480200_undelivered_or_fyb) +
        SUM(ussgl488100_upward_adjustm_cpe) +
        SUM(ussgl488200_upward_adjustm_cpe) +
        SUM(ussgl490100_delivered_orde_cpe) - SUM(ussgl490100_delivered_orde_fyb) +
        SUM(ussgl490200_delivered_orde_cpe) +
        SUM(ussgl490800_authority_outl_cpe) - SUM(ussgl490800_authority_outl_fyb) +
        SUM(ussgl498100_upward_adjustm_cpe) +
        SUM(ussgl498200_upward_adjustm_cpe)
    ) <> (-1 * sf.amount);
