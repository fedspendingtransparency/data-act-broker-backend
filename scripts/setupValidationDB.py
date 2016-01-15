from dataactcore.scripts.databaseSetup import runCommands
from dataactcore.models.validationInterface import ValidationInterface

def setupValidationDB( hardReset = False):
    """ Clear validation DB if it exists, then create all tables and insert file types, rule types, and field types """

    hardResetSQL = [
        "DROP TABLE IF EXISTS rule",
        "DROP TABLE IF EXISTS file_columns",
        "DROP TABLE IF EXISTS field_type",
        "DROP TABLE IF EXISTS rule_type",
        "DROP TABLE IF EXISTS file_type",
        "DROP SEQUENCE IF EXISTS fileIdSerial",
        "DROP SEQUENCE IF EXISTS ruleTypeSerial",
        "DROP SEQUENCE IF EXISTS fieldTypesSerial",
        "DROP SEQUENCE IF EXISTS fileColumnSerial",
        "DROP SEQUENCE IF EXISTS ruleIdSerial"
    ]

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
        "CREATE TABLE rule (rule_id integer PRIMARY KEY DEFAULT nextval('ruleIdSerial'), file_column_id integer REFERENCES file_columns, rule_type_id integer REFERENCES rule_type,rule_text_1 text,rule_text_2 text,description text);",

        "INSERT INTO file_type (file_id,name, description) VALUES (1, 'award', 'award file'), (2, 'award_financial', 'award_financial file'), (3, 'appropriations', 'appropriations file'), (4, 'procurement','procurement file');",
        "INSERT INTO rule_type (rule_type_id, name,description) VALUES (1, 'TYPE', 'checks type'), (2, 'EQUAL', 'equals operatior '),(3, 'NOT EQUAL', 'not equals operator '), (4, 'LESS', 'less than operator '), (5, 'GREATER', 'greater than operator'), (6, 'LENGTH', 'string length');",
        "INSERT INTO field_type (field_type_id ,name,description) VALUES (1, 'INT', 'integer type'), (2, 'DECIMAL', 'decimal type '),(3, 'BOOLEAN', 'yes/no'), (4, 'STRING', 'string type');"
    ]
    if(hardReset) :
        runCommands(ValidationInterface.getCredDict(),hardResetSQL,"validation")
    runCommands(ValidationInterface.getCredDict(),sql,"validation")

if __name__ == '__main__':
    setupValidationDB(False)
