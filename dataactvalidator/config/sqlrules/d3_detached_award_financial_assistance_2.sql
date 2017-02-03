-- ActionType field must contain one of the following values: “A”, “B”, “C” or “D”.
-- Can also be '' if RecordType = 1, so that's included as well.
SELECT
    dafa.row_number,
    dafa.action_type,
    dafa.record_type
FROM detached_award_financial_assistance as dafa
WHERE LOWER(COALESCE(dafa.action_type, '')) NOT IN ('', 'a', 'b', 'c', 'd') ;
