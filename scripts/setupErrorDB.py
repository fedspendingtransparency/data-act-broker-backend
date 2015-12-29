from dataactcore.scripts.databaseSetup import runCommands
from dataactcore.models.errorInterface import ErrorInterface

sqlStatements=[
    "CREATE TABLE status (status_id integer PRIMARY KEY, name text NOT NULL,description text NOT NULL);",
    "CREATE TABLE error_type (error_type_id integer PRIMARY KEY, name text NOT NULL,description text NOT NULL);",
    "CREATE SEQUENCE fileSerial START 1;",
    "CREATE TABLE file_status (file_id integer PRIMARY KEY DEFAULT nextval('fileSerial'), job_id integer NOT NULL, filename text, status_id integer REFERENCES status NOT NULL);",
    "CREATE SEQUENCE errorDataSerial START 1;",
    "CREATE TABLE error_data (error_data_id integer PRIMARY KEY DEFAULT nextval('errorDataSerial'), job_id integer NOT NULL, filename text, field_name text NOT NULL, error_type_id integer REFERENCES error_type, occurrences integer NOT NULL, first_row integer NOT NULL, rule_failed text);",
    "INSERT INTO status (status_id,name, description) VALUES (1, 'complete', 'File has been processed'), (2, 'missing_header_error', 'One of the required columns is not present in the file'), (3, 'bad_header_error', 'One of the headers in the file is not recognized'), (4, 'unknown_error', 'An unknown error has occurred with this file'), (5,'single_row_error','CSV file must have a header row and at least one record'), (6,'duplicate_error','May not have the same header twice'), (7,'job_error','Error occurred in job manager');",
    "INSERT INTO error_type (error_type_id,name, description) VALUES (1, 'type_error', 'The value provided was of the wrong type'), (2, 'required_error', 'A required value was not provided'), (3, 'value_error', 'The value provided was invalid'),(4, 'read_error', 'Could not parse this record correctly'),(5, 'write_error', 'Could not write this record into the staging database');"]

runCommands(ErrorInterface.getCredDict(),sqlStatements,"error_data")