-- ALTER TABLE raw.defc
-- DROP COLUMN test_column;

CREATE TABLE raw.defc_copy AS
SELECT * EXCEPT (test_column)
FROM raw.defc;

DROP TABLE raw.defc;

ALTER TABLE raw.defc_copy RENAME TO raw.defc;
