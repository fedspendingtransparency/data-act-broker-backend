SELECT
	af.row_number,
	af.piid,
    af.parent_award_id
FROM award_financial AS af
WHERE af.submission_id = {}
	AND NOT EXISTS (
		SELECT 1
		FROM award_procurement AS ap
		WHERE ap.submission_id = af.submission_id
		    AND ap.piid = af.piid
		    AND ap.parent_award_id = af.parent_award_id
	)