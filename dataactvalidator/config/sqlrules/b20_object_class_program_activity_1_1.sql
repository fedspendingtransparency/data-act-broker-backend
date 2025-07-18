-- All combinations of TAS, program activity code+name, object class, DEFC, and PYA in File C (award financial) must
-- exist in File B (object class program activity). Since not all object classes will have award activity, it is
-- acceptable for combinations of TAS, program activity code+name, object class, and DEFC combination where PYA = "X" or
-- NULL in File C to be a subset of those provided in File B.

WITH award_financial_b20_{0} AS
    (SELECT row_number,
        submission_id,
        program_activity_code,
        program_activity_name,
        object_class,
        account_num,
        disaster_emergency_fund_code,
        display_tas,
        prior_year_adjustment
    FROM award_financial
    WHERE submission_id = {0}
        AND UPPER(prior_year_adjustment) = 'X'
        AND (COALESCE(program_activity_code, '') <> ''
            OR COALESCE(program_activity_name, '') <> '')),
ocpa_b20_{0} AS
    (SELECT account_num,
        program_activity_code,
        program_activity_name,
        object_class,
        disaster_emergency_fund_code,
        prior_year_adjustment
    FROM object_class_program_activity
    WHERE submission_id = {0}
        AND UPPER(prior_year_adjustment) = 'X'
        AND (COALESCE(program_activity_code, '') <> ''
            OR COALESCE(program_activity_name, '') <> ''))
SELECT
    af.row_number AS "source_row_number",
    af.display_tas AS "source_value_tas",
    af.program_activity_code AS "source_value_program_activity_code",
    af.program_activity_name AS "source_value_program_activity_name",
    af.object_class AS "source_value_object_class",
    af.disaster_emergency_fund_code AS "source_value_disaster_emergency_fund_code",
    af.prior_year_adjustment AS "source_value_prior_year_adjustment",
    af.display_tas AS "uniqueid_TAS",
    af.program_activity_code AS "uniqueid_ProgramActivityCode",
    af.program_activity_name AS "uniqueid_ProgramActivityName",
    af.object_class AS "uniqueid_ObjectClass",
    af.disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode"
FROM award_financial_b20_{0} AS af
JOIN submission AS sub
    ON sub.submission_id = af.submission_id
WHERE NOT EXISTS (
        SELECT 1
        FROM ocpa_b20_{0} AS op
        WHERE COALESCE(af.account_num, 0) = COALESCE(op.account_num, 0)
            AND (COALESCE(af.program_activity_code, '') = COALESCE(op.program_activity_code, '')
                OR COALESCE(af.program_activity_code, '') = ''
                OR af.program_activity_code = '0000'
            )
            AND (UPPER(COALESCE(af.program_activity_name, '')) = UPPER(COALESCE(op.program_activity_name, ''))
                OR COALESCE(af.program_activity_name, '') = ''
            )
            AND (COALESCE(af.object_class, '') = COALESCE(op.object_class, '')
                OR (af.object_class IN ('0', '00', '000', '0000')
                    AND op.object_class IN ('0', '00', '000', '0000')
                )
            )
            AND UPPER(COALESCE(af.disaster_emergency_fund_code, '')) = UPPER(COALESCE(op.disaster_emergency_fund_code, ''))
            AND UPPER(af.prior_year_adjustment) = UPPER(op.prior_year_adjustment)
    )
    AND NOT (UPPER(af.program_activity_code) = 'OPTN'
        AND UPPER(af.program_activity_name) = 'FIELD IS OPTIONAL PRIOR TO FY21'
        AND sub.reporting_fiscal_year < 2021);
