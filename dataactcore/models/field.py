class FieldType:
    """ Acts as an enum for field types """
    INTEGER = "INTEGER"
    TEXT = "TEXT"

class FieldConstraint:
    """ Acts a an enum for field constraints """
    NONE = ""
    PRIMARY_KEY = "PRIMARY KEY"
    NOT_NULL = "NOT NULL"