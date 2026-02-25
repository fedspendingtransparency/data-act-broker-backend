-- All the Direct Appropriation (D) amounts reported for (4801_CPE less 4801_FYB) + (4802_CPE less 4802_FYB)
-- + 4881_CPE + 4882_CPE + (4901_CPE less 4901_FYB) + 4902_CPE + (4908_CPE less 4908_FYB) + 4981_CPE + 4982_CPE =
-- the opposite sign of GTAS SF 133 line 2004 per TAS, for the same reporting period and TAS and DEFC combination
-- where PYA = "X".
WITH object_class_program_activity_b14_{0} AS
    (SELECT sub.reporting_fiscal_year,
        sub.reporting_fiscal_period,
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
        ) AS total
    FROM object_class_program_activity AS op
    JOIN submission sub ON sub.submission_id = op.submission_id
    WHERE op.submission_id = {0}
        AND UPPER(prior_year_adjustment) = 'X'
        AND UPPER(by_direct_reimbursable_fun) = 'D'
    GROUP BY UPPER(disaster_emergency_fund_code),
        display_tas,
        UPPER(prior_year_adjustment),
        sub.reporting_fiscal_year,
        sub.reporting_fiscal_period),
sf_133_b14_{0} AS (
    SELECT display_tas,
        UPPER(disaster_emergency_fund_code) AS disaster_emergency_fund_code,
        UPPER(prior_year_adjustment) AS prior_year_adjustment,
        SUM(amount) AS total
    FROM sf_133
    WHERE EXISTS (
            SELECT 1
            FROM object_class_program_activity_b14_{0} AS op
            WHERE sf_133.period = op.reporting_fiscal_period
                AND sf_133.fiscal_year = op.reporting_fiscal_year
        )
        AND line = 2004
    GROUP BY UPPER(disaster_emergency_fund_code),
        display_tas,
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
    COALESCE(sf.total, 0) AS "expected_value_GTAS SF133 Line 2004",
    op.total + COALESCE(sf.total, 0) AS "difference",
    op.display_tas AS "uniqueid_TAS",
    UPPER(op.disaster_emergency_fund_code) AS "uniqueid_DisasterEmergencyFundCode"
FROM object_class_program_activity_b14_{0} AS op
LEFT OUTER JOIN sf_133_b14_{0} AS sf
    ON op.display_tas = sf.display_tas
    AND op.disaster_emergency_fund_code = COALESCE(sf.disaster_emergency_fund_code, '')
WHERE op.total <> (-1 * COALESCE(sf.total, 0));