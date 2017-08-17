-- BusinessTypes must be one to three letters in length. BusinessTypes values must be non-repeated letters from A to X.
SELECT
    row_number,
    business_types
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (business_types ~* '([A-X]).*\1'
	    OR (business_types !~* '^[A-X]$'
	        AND business_types !~* '^[A-X][A-X]$'
	        AND business_types !~* '^[A-X][A-X][A-X]$'))