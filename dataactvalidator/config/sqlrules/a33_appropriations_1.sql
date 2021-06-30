-- Verify that all of the applicable GTASes have an associated entry in the submission (File A (appropriation)).
-- Each TAS reported to GTAS for SF-133 should be reported in File A, with the exception of Financing Accounts, or
-- when all monetary amounts are zero for the TAS.

-- Note: This logic should exactly match the logic used to generate file A
WITH frec_list AS (
    SELECT cgac_code,
        array_agg(frec.frec_code) AS "frec_list"
    FROM cgac
    JOIN frec
        ON cgac.cgac_id=frec.cgac_id
    GROUP BY cgac.cgac_code
),
cgac_exceptions AS (
	SELECT *
	FROM (VALUES
	    ('097', ARRAY ['017', '021', '057', '097']),
	    ('1601', ARRAY ['1601', '016']),
	    ('1125', ARRAY ['1125', '011'])
	) AS t (agency_code, associated_codes)
),
sub_{0}_combo AS (
    SELECT sub.cgac_code,
        sub.frec_code,
        sub.reporting_fiscal_period,
        sub.reporting_fiscal_year,
        COALESCE(ce.associated_codes, ARRAY [COALESCE(sub.cgac_code, sub.frec_code)]) AS "associated_codes",
        fl.frec_list AS "frec_list"
    FROM submission AS sub
    LEFT JOIN cgac_exceptions AS ce
        ON COALESCE(sub.cgac_code, sub.frec_code) = ce.agency_code
    LEFT JOIN frec_list AS fl
        ON sub.cgac_code = fl.cgac_code
    WHERE sub.submission_id = {0}
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
    LEFT JOIN tas_lookup AS tl
        ON tl.account_num = sf.account_num
    JOIN sub_{0}_combo AS sub_c
        ON sf.period = sub_c.reporting_fiscal_period
        AND sf.fiscal_year = sub_c.reporting_fiscal_year
        AND (
            -- ATA filter, should only apply for CGACs (and FREC in cgac_exceptions) as it's always 3 digits
            sf.allocation_transfer_agency = ANY(sub_c.associated_codes)
            -- AID filter, should only apply for CGACs as it's always 3 digits
            -- fr_entity_type should only apply for FRECs as it's always 4 digits
            OR (sf.allocation_transfer_agency IS NULL
                AND CASE WHEN sub_c.cgac_code IS NOT NULL
                    THEN sf.agency_identifier = ANY(sub_c.associated_codes)
                    ELSE tl.fr_entity_type = sub_c.frec_code
                    END)
            -- match against FRECs related to CGAC 011
            OR sub_c.frec_list IS NOT NULL
                AND sub_c.cgac_code IS NOT NULL
                AND sf.allocation_transfer_agency IS NULL
                AND sf.agency_identifier = '011'
                AND tl.fr_entity_type = ANY(sub_c.frec_list)
        )
WHERE NOT EXISTS (
        SELECT 1
        FROM appropriation AS approp
        WHERE sf.tas IS NOT DISTINCT FROM approp.tas
            AND approp.submission_id = {0}
    )
    AND COALESCE(UPPER(tl.financial_indicator2), '') <> 'F';
