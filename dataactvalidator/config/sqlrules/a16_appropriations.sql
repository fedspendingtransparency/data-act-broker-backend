-- All the elements that have FYB in file A (appropriation) are expected in quarter 1.
WITH appropriation_a16_{0} AS
	(SELECT submission_id,
		row_number,
		budget_authority_unobligat_fyb
	FROM appropriation
	WHERE submission_id = {0})
SELECT
    row_number,
	budget_authority_unobligat_fyb
FROM appropriation_a16_{0} AS approp
	JOIN submission AS sub
	    ON sub.submission_id = approp.submission_id
WHERE NOT EXISTS (
        SELECT 1
        FROM submission AS query_sub
		    LEFT JOIN publish_status AS ps
		    ON query_sub.publish_status_id = ps.publish_status_id
		WHERE query_sub.submission_id <> {0}
		    AND query_sub.cgac_code = sub.cgac_code
		    AND query_sub.reporting_fiscal_year = sub.reporting_fiscal_year
		    AND (ps.name IN ('published','updated')
		        OR query_sub.publishable = TRUE
		    )
    )
	AND budget_authority_unobligat_fyb IS NULL;
