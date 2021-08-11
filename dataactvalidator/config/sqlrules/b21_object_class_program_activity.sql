-- For monthly submissions, all TAS and COVID-19 Disaster Emergency Fund Code combinations required to be reported to
-- GTAS are reported in File B, with the exception of Financing Accounts (or when all obligation and outlay monetary
-- amounts are zero for the TAS). As noted in A33, allocation accounts should be reported by the Child Agency, not by
-- the Parent agency, and the AllocationTransferAgencyIdentifier listed should match the Common Government-wide
-- Accounting Classification (CGAC) of the submitting agency.
WITH limited_lines_sf_133_b21_{0} AS
    (SELECT *
    FROM sf_133
    WHERE line IN (2190, 3020)
        AND amount <> 0)
SELECT DISTINCT
    NULL AS row_number,
    sf.allocation_transfer_agency,
    sf.agency_identifier,
    sf.beginning_period_of_availa,
    sf.ending_period_of_availabil,
    sf.availability_type_code,
    sf.main_account_code,
    sf.sub_account_code,
    sf.disaster_emergency_fund_code,
    sf.display_tas AS "uniqueid_TAS",
    sf.disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode"
FROM limited_lines_sf_133_b21_{0} AS sf
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
    AND sub.is_quarter_format IS FALSE
    AND NOT EXISTS (
        SELECT 1
        FROM object_class_program_activity AS op
        WHERE sf.tas IS NOT DISTINCT FROM op.tas
            AND COALESCE(sf.disaster_emergency_fund_code, '') = UPPER(op.disaster_emergency_fund_code)
            AND op.submission_id = {0}
    )
    AND COALESCE(UPPER(tas_lookup.financial_indicator2), '') <> 'F';
