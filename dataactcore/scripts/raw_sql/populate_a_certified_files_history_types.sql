UPDATE certified_files_history
SET file_type_id = 1
WHERE warning_filename ~ '_appropriations_warning_report.csv$'
  AND file_type_id IS NULL;
