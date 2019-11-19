-- ContractAuthorityAmountTotal_CPE = CPE aggregate value for GTAS SF-133 line #1540 + #1640
WITH appropriation_a9_{0} AS
    (SELECT row_number,
        contract_authority_amount_cpe,
        tas,
        display_tas,
        submission_id
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.contract_authority_amount_cpe,
    SUM(sf.amount) AS "expected_value_SUM of GTAS SF133 Lines 1540, 1640",
    COALESCE(approp.contract_authority_amount_cpe, 0) - SUM(sf.amount) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation_a9_{0} AS approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line IN (1540, 1640)
GROUP BY approp.row_number,
    approp.contract_authority_amount_cpe,
    approp.display_tas
HAVING COALESCE(approp.contract_authority_amount_cpe, 0) <> SUM(sf.amount);
