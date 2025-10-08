-- AdjustmentsToUnobligatedBalanceBroughtForward_CPE= CPE aggregate value for GTAS SF-133 line #1010 through 1067
-- (periods prior to FY21 will continue to SUM lines 1010 through 1042)
WITH appropriation_a12_{0} AS
    (SELECT submission_id,
        row_number,
        adjustments_to_unobligated_cpe,
        display_tas
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.adjustments_to_unobligated_cpe,
    SUM(sf.amount) AS "expected_value_SUM of GTAS SF133 Lines 1010 through 1067",
    approp.adjustments_to_unobligated_cpe - SUM(sf.amount) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation_a12_{0} AS approp
    INNER JOIN sf_133 AS sf
        ON approp.display_tas = sf.display_tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line >= 1010
    AND ((sf.line <= 1042
            AND sf.fiscal_year <= 2020
        )
        OR (sf.line <= 1067
            AND sf.fiscal_year > 2020
        )
    )
GROUP BY approp.row_number,
    approp.adjustments_to_unobligated_cpe,
    approp.display_tas
HAVING approp.adjustments_to_unobligated_cpe <> SUM(sf.amount);
