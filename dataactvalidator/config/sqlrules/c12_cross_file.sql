SELECT
    ap.row_number,
    ap.piid,
    ap.parent_award_id
FROM award_procurement AS ap
WHERE ap.submission_id = {}
    AND ap.piid IS NOT NULL
    AND (ap.federal_action_obligation IS NOT NULL
        AND ap.federal_action_obligation <> ''
        AND ap.federal_action_obligation <> '0')
    AND NOT EXISTS (
		SELECT 1
		FROM award_financial AS af
		WHERE af.submission_id = ap.submission_id
		    AND af.piid = ap.piid
		    AND af.parent_award_id IS NOT DISTINCT FROM ap.parent_award_id
	)
