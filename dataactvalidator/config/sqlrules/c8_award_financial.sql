-- Unique FAIN/URI from file C exists in file D2. FAIN may be null for aggregated records.
-- URI may be null for non-aggregated records.
WITH award_financial_c8_{0} AS
    (SELECT submission_id,
        row_number,
        allocation_transfer_agency,
        agency_identifier,
        transaction_obligated_amou,
        fain,
        uri
    FROM award_financial
    WHERE submission_id = {0}),
award_financial_assistance_c8_{0} AS
    (SELECT submission_id,
        row_number,
        fain,
        uri
    FROM award_financial_assistance
    WHERE submission_id = {0})
SELECT
    af.row_number,
    af.fain,
    af.uri
FROM award_financial_c8_{0} AS af
WHERE af.transaction_obligated_amou IS NOT NULL
    AND (COALESCE(af.allocation_transfer_agency, '') = ''
        OR (COALESCE(af.allocation_transfer_agency, '') != ''
            AND af.allocation_transfer_agency = af.agency_identifier
        )
        OR (COALESCE(af.allocation_transfer_agency, '') != ''
            AND af.allocation_transfer_agency != af.agency_identifier
            AND NOT EXISTS (
                SELECT 1
                FROM cgac
                WHERE cgac_code = af.allocation_transfer_agency)
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM cgac
        WHERE cgac_code = af.allocation_transfer_agency
    )
    AND (af.fain IS NOT NULL
        OR af.uri IS NOT NULL
    )
    AND NOT EXISTS (
        SELECT 1
        FROM award_financial_assistance_c8_{0} AS afa
        WHERE af.fain = afa.fain
    )
    AND NOT EXISTS (
        SELECT 1
        FROM award_financial_assistance_c8_{0} AS afa
        WHERE af.uri = afa.uri
    );
