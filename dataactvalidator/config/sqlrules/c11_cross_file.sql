-- Each unique PIID (or combination of PIID/ParentAwardId) from file C (award financial) should exist in
-- file D1 (award procurement).
WITH award_financial_c11_{0} AS
    (SELECT transaction_obligated_amou,
        piid,
        parent_award_id,
        row_number,
        allocation_transfer_agency
    FROM award_financial
    WHERE submission_id = {0}),
award_procurement_c11_{0} AS
    (SELECT piid,
        parent_award_id
    FROM award_procurement
    WHERE submission_id = {0})
SELECT
    af.row_number,
    af.piid,
    af.parent_award_id
FROM award_financial_c11_{0} AS af
WHERE af.transaction_obligated_amou IS NOT NULL
    AND af.piid IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM cgac
        WHERE cgac_code = af.allocation_transfer_agency
    )
    AND ((af.parent_award_id IS NULL
            AND NOT EXISTS (
              SELECT 1
              FROM award_procurement_c11_{0} AS ap
              WHERE ap.piid = af.piid
            )
        )
         OR (af.parent_award_id IS NOT NULL
             AND NOT EXISTS (
                 SELECT 1
                 FROM award_procurement_c11_{0} AS ap
                 WHERE ap.piid = af.piid
                     AND COALESCE(ap.parent_award_id, '') = COALESCE(af.parent_award_id, '')
             )
         )
    );
