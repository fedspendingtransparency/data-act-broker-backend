-- Validation compares piid and/or program parent_award_id
SELECT
    af.row_number,
    af.piid,
    af.parent_award_id
FROM award_financial AS af
WHERE af.submission_id = {}
    AND af.transaction_obligated_amou IS NOT NULL
    AND af.piid IS NOT NULL
    AND ((af.parent_award_id IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM award_procurement AS ap
              WHERE ap.submission_id = af.submission_id
                  AND ap.piid = af.piid)
         )
         OR (af.parent_award_id IS NOT NULL
             AND NOT EXISTS (
                 SELECT 1
                 FROM award_procurement AS ap
                 WHERE ap.submission_id = af.submission_id
                     AND ap.piid = af.piid
                     AND ap.parent_award_id IS NOT DISTINCT FROM af.parent_award_id)
         )
    )
    AND NOT EXISTS (
        SELECT cgac_code
        FROM cgac
        WHERE cgac_code = af.allocation_transfer_agency
    )