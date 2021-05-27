-- Verify that all of the applicable GTASes have an associated entry in the submission (File A (appropriation)).
-- Each TAS reported to GTAS for SF-133 should be reported in File A, with the exception of Financing Accounts, or
-- when all monetary amounts are zero for the TAS.
WITH sub_cgac_frec_combo_a33_1_{0} AS
    (SELECT sub.submission_id,
        sub.reporting_fiscal_period,
        sub.reporting_fiscal_year,
        COALESCE(sub.cgac_code, cgac.cgac_code) AS "agency_code"
     FROM submission AS sub
     LEFT JOIN frec ON sub.frec_code=frec.frec_code
     LEFT JOIN cgac ON frec.cgac_id=cgac.cgac_id
    )
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
    JOIN sub_cgac_frec_combo_a33_1_{0} AS sub_combo
        ON sf.period = sub_combo.reporting_fiscal_period
        AND sf.fiscal_year = sub_combo.reporting_fiscal_year
        AND ((sf.agency_identifier = sub_combo.agency_code
                AND sf.allocation_transfer_agency IS NULL
            )
            OR sf.allocation_transfer_agency = sub_combo.agency_code
        )
    LEFT JOIN tas_lookup
        ON tas_lookup.account_num = sf.account_num
WHERE sub_combo.submission_id = {0}
    AND NOT EXISTS (
        SELECT 1
        FROM appropriation AS approp
        WHERE sf.tas IS NOT DISTINCT FROM approp.tas
            AND approp.submission_id = {0}
    )
    AND COALESCE(UPPER(tas_lookup.financial_indicator2), '') <> 'F';
