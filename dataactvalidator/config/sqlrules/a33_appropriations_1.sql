-- Verify that all of the applicable GTASes have an associated entry in the submission (File A (appropriation)).
-- Each TAS reported to GTAS for SF-133 should be reported in File A, with the exception of Financing Accounts, or
-- when all monetary amounts are zero for the TAS.
SELECT DISTINCT
    NULL AS row_number,
    sf.allocation_transfer_agency,
    sf.agency_identifier,
    sf.beginning_period_of_availa,
    sf.ending_period_of_availabil,
    sf.availability_type_code,
    sf.main_account_code,
    sf.sub_account_code,
    sf.display_tas AS "uniqueid_TAS"
FROM sf_133 AS sf
    JOIN submission AS sub
        ON sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
        AND ((sf.agency_identifier = sub.cgac_code
                AND sf.allocation_transfer_agency IS NULL
            )
            OR sf.allocation_transfer_agency = sub.cgac_code
        )
    LEFT JOIN tas_lookup
        ON tas_lookup.account_num = sf.account_num
WHERE sub.submission_id = {0}
    AND NOT EXISTS (
        SELECT 1
        FROM appropriation AS approp
        WHERE sf.tas IS NOT DISTINCT FROM approp.tas
            AND approp.submission_id = {0}
    )
    AND COALESCE(UPPER(tas_lookup.financial_indicator2), '') <> 'F';
