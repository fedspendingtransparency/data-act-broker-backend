import json
from dataactcore.scripts.databaseSetup import runCommands

sql=[
"CREATE SEQUENCE fileIdSerial START 1;",
"CREATE TABLE file_type (file_id integer PRIMARY KEY DEFAULT nextval('fileIdSerial'), name text ,description text)",


"CREATE SEQUENCE ruleTypeSerial START 1;",
"CREATE TABLE rule_type (rule_type_id integer PRIMARY KEY DEFAULT nextval('ruleTypeSerial'), name text,description text);",



"CREATE SEQUENCE fieldTypesSerial START 1;",
"CREATE TABLE field_type (field_type_id integer PRIMARY KEY DEFAULT nextval('fieldTypesSerial'), name text,description text);",


"CREATE SEQUENCE fileColumnSerial START 1;",
"CREATE TABLE file_columns (file_column_id integer PRIMARY KEY DEFAULT nextval('fileColumnSerial'), file_id integer REFERENCES file_type,field_types_id integer REFERENCES field_type , name text ,description text , required  boolean);",

"CREATE SEQUENCE ruleIdSerial START 1;",
"CREATE TABLE rule (rule_id integer PRIMARY KEY DEFAULT nextval('ruleIdSerial'), file_column_id integer REFERENCES file_columns, rule_type_id integer REFERENCES rule_type,rule_text_1 text,rule_text_2 text);",


"INSERT INTO file_type (file_id,name, description) VALUES (1, 'appropriations', 'appropriations file'), (2, 'award_financial', 'award_financial file'), (3, 'award', 'award file'), (4, 'procurement','procurement file');",
"INSERT INTO rule_type (rule_type_id, name,description) VALUES (1, 'TYPE', 'checks type'), (2, 'EQUAL', 'equals operatior '),(3, 'NOT EQUAL', 'not equals operator '), (4, 'LESS', 'less than operator '), (5, 'GREATER', 'greater than operator'), (6, 'LENGTH', 'string length');",
"INSERT INTO field_type (field_type_id ,name,description) VALUES (1, 'INT', 'integer type'), (2, 'DECIMAL', 'decimal type '),(3, 'BOOLEAN', 'yes/no'), (4, 'STRING', 'string type');"
]

config = json.loads(open("dbCred.json","r").read())
runCommands(config,sql,"validation")
