-- Validation compares piid and/or program parent_award_id
SELECT
    ap.row_number,
    ap.piid,
    ap.parent_award_id
FROM award_procurement AS ap
WHERE ap.submission_id = {}
    AND COALESCE(ap.federal_action_obligation, 0) <> 0
    AND ap.piid IS NOT NULL
    AND ((ap.parent_award_id IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM award_financial AS af
              WHERE af.submission_id = ap.submission_id
                  AND af.piid = ap.piid)
         )
         OR (ap.parent_award_id IS NOT NULL
             AND NOT EXISTS (
                 SELECT 1
                 FROM award_financial AS af
                 WHERE af.submission_id = ap.submission_id
                     AND af.piid = ap.piid
                     AND af.parent_award_id IS NOT DISTINCT FROM ap.parent_award_id)
         )
    )
    AND NOT EXISTS (
        SELECT af.transaction_obligated_amou
        FROM award_financial AS af
        WHERE af.transaction_obligated_amou IS NULL
    )
