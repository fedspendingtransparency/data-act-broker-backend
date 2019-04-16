-- LegalEntityCountryCode Field must contain a valid three character GENC Standard Edition 3.0 (Update 4) country code.
-- U.S. Territories and Freely Associated States must be submitted with country code = USA and their state code.
-- They cannot be submitted with their GENC country code. See Appendix B of the Practices and Procedures.
SELECT
    dafa.row_number,
    dafa.legal_entity_country_code
FROM detached_award_financial_assistance AS dafa
WHERE submission_id={0}
    AND NOT EXISTS (
        SELECT 1
        FROM country_code AS cc
        WHERE UPPER(dafa.legal_entity_country_code) = UPPER(cc.country_code)
    );
