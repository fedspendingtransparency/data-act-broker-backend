-- BorrowingAuthorityAmountTotal_CPE = CPE aggregate value for GTAS SF-133 line #1340 + #1440
WITH appropriation_a10_{0} AS
    (SELECT submission_id,
        row_number,
        borrowing_authority_amount_cpe,
        tas
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.borrowing_authority_amount_cpe,
    SUM(sf.amount) AS "expected_value_SUM of GTAS SF133 Lines 1340, 1440",
    COALESCE(approp.borrowing_authority_amount_cpe, 0) - SUM(sf.amount) AS "difference",
    approp.tas AS "uniqueid_TAS"
FROM appropriation_a10_{0} AS approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line IN (1340, 1440)
GROUP BY approp.row_number,
    approp.borrowing_authority_amount_cpe,
    approp.tas
HAVING COALESCE(approp.borrowing_authority_amount_cpe, 0) <> SUM(sf.amount);
