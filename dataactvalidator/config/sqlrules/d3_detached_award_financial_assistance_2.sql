-- ActionType field must contain A, B, C, D, or blank
SELECT
    dafa.row_number,
    dafa.action_type,
    dafa.record_type
FROM detached_award_financial_assistance as dafa
WHERE LOWER(COALESCE(dafa.action_type, '')) NOT IN ('', 'a', 'b', 'c', 'd') ;
