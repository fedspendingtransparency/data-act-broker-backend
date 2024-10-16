-- All combinations of TAS, PARK, object class, DEFC, and PYA in File C (award financial) must exist in File B (object
-- class program activity). Since not all object classes will have award activity, it is acceptable for combinations of
-- TAS, PARK, object class, and DEFC combination where PYA = X or NULL in File C to be a subset of those provided in
-- File B. If PYA is not provided in File C, the combination of TAS, PARK, object class, and DEFC is applied.

WITH award_financial_b20_{0} AS
    (SELECT row_number,
        submission_id,
        pa_reporting_key,
        object_class,
        account_num,
        disaster_emergency_fund_code,
        display_tas
    FROM award_financial
    WHERE submission_id = {0}
        AND COALESCE(prior_year_adjustment, '') = ''
        AND COALESCE(pa_reporting_key, '') <> ''),
ocpa_b20_{0} AS
    (SELECT account_num,
        pa_reporting_key,
        object_class,
        disaster_emergency_fund_code
    FROM object_class_program_activity
    WHERE submission_id = {0}
        AND COALESCE(pa_reporting_key, '') <> '')
SELECT
    af.row_number AS "source_row_number",
    af.display_tas AS "source_value_TAS",
    af.pa_reporting_key AS "source_value_pa_reporting_key",
    af.object_class AS "source_value_object_class",
    af.disaster_emergency_fund_code AS "source_value_disaster_emergency_fund_code",
    af.display_tas AS "uniqueid_TAS",
    af.pa_reporting_key AS "uniqueid_ProgramActivityReportingKey",
    af.object_class AS "uniqueid_ObjectClass",
    af.disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode"
FROM award_financial_b20_{0} AS af
WHERE NOT EXISTS (
        SELECT 1
        FROM ocpa_b20_{0} AS op
        WHERE COALESCE(af.account_num, 0) = COALESCE(op.account_num, 0)
            AND UPPER(af.pa_reporting_key) = UPPER(op.pa_reporting_key)
            AND (COALESCE(af.object_class, '') = COALESCE(op.object_class, '')
                OR (af.object_class IN ('0', '00', '000', '0000')
                    AND op.object_class IN ('0', '00', '000', '0000')
                )
            )
            AND UPPER(COALESCE(af.disaster_emergency_fund_code, '')) = UPPER(COALESCE(op.disaster_emergency_fund_code, ''))
    );
