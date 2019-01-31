UPDATE certified_files_history
SET file_type_id = 2
WHERE warning_filename ~ '_program_activity_warning_report.csv$'
  AND file_type_id IS NULL;
