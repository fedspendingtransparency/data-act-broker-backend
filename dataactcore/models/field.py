class FieldType:
    """ Acts as an enum for field types """
    INTEGER = 0
    TEXT = 1

class FieldConstraint:
    """ Acts a an enum for field constraints """
    NONE = 0
    PRIMARY_KEY = 1
    NOT_NULL = 2