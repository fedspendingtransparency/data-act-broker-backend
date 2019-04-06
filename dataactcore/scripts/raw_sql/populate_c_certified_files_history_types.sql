UPDATE certified_files_history
SET file_type_id = 3
WHERE warning_filename ~ '_award_financial_warning_report.csv$'
  AND file_type_id IS NULL;
