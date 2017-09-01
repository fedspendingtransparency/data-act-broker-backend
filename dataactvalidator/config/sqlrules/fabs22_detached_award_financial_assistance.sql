-- When provided, CorrectionLateDeleteIndicator must contain one of the following values: ""C"", ""D"", or ""L"".
SELECT
    row_number,
    correction_late_delete_ind
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(UPPER(correction_late_delete_ind),'') not in ('','C','D','L')