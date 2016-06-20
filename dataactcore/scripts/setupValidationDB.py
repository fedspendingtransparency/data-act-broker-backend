from dataactcore.scripts.databaseSetup import createDatabase, runMigrations
from dataactcore.config import CONFIG_DB
from dataactcore.models.validationModels import FileType, RuleType, FieldType, RuleTiming, RuleSeverity
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface

def setupValidationDB():
    """Create validation tables from model metadata and do initial inserts."""
    createDatabase(CONFIG_DB['validator_db_name'])
    runMigrations('validation')
    insertCodes()

def insertCodes():
    """Insert static data."""
    validatorDb = ValidatorValidationInterface()

    # insert rule timing
    ruleTimingList = [
        (1, 'file_validation', 'Run during pre-load validation of a file'),
        (2, 'prerequisite', 'Run only when referenced by another rule'),
        (3, 'cross_file', 'This rule is checked during cross-file validation'),
        (4, 'multi_field', 'This rule is run after field level validations are complete')
        ]
    for r in ruleTimingList:
        ruleTiming = RuleTiming(
            rule_timing_id=r[0], name=r[1], description=r[2])
        validatorDb.session.merge(ruleTiming)

    # insert file types
    fileTypeList = [
        (1, 'award', 'award file'),
        (2, 'award_financial', 'award_financial file'),
        (3, 'appropriations', 'appropriations file'),
        (4, 'program_activity','program activity and object class file')
        ]
    for f in fileTypeList:
        fileType = FileType(file_id=f[0], name=f[1], description=f[2])
        validatorDb.session.merge(fileType)

    # insert rule types
    ruleTypeList = [(1, 'TYPE', 'checks type'),
        (2, 'EQUAL', 'equals operatior '),
        (3, 'NOT_EQUAL', 'not equals operator '),
        (4, 'LESS', 'less than operator '),
        (5, 'GREATER', 'greater than operator'),
        (6, 'LENGTH', 'string length'),
        (7, 'IN_SET', 'value must be in set'),
        (8, 'MIN_LENGTH', 'length of data must be at least reference value'),
        (9, 'REQUIRED_CONDITIONAL', 'field is required if secondary rule passes'),
        (10, 'SUM', 'field is equal to the sum of other fields'),
        (11, 'CAR_MATCH', 'Matching a set of fields against a CAR file'),
        (12, 'FIELD_MATCH', 'Match a set of fields against a different file'),
        (13, 'RULE_IF', 'Apply first rule if second rule passes'),
        (14, 'SUM_TO_VALUE', 'Sum a set of fields and compare to specified value'),
        (15, 'REQUIRE_ONE_OF_SET', 'At least one of these fields must be present'),
        (16, 'SUM_FIELDS', 'Field is equal to the sum of other fields'),
        (17, 'NOT', 'passes if and only if specified rule fails'),
        (18, 'SUM_BY_TAS', 'Check if two fields summed by TAS are equal'),
        (19, 'EXISTS_IN_TABLE', 'Check that value exists in specified table'),
        (20, 'REQUIRED_SET_CONDITIONAL', 'Check that all fields in set are present if conditional rule passes'),
        (21, 'CHECK_PREFIX', 'Check first character against a value in another field'),
        (22, 'SET_EXISTS_IN_TABLE', 'Check whether set of values exists in specified table')
        ]
    for r in ruleTypeList:
        ruleType = RuleType(rule_type_id=r[0], name=r[1], description=r[2])
        validatorDb.session.merge(ruleType)

    # insert field types
    fieldTypeList = [
        (1, 'INT', 'integer type'),
        (2, 'DECIMAL', 'decimal type '),
        (3, 'BOOLEAN', 'yes/no'),
        (4, 'STRING', 'string type'),
        (5, 'LONG', 'long integer')
        ]
    for f in fieldTypeList:
        fieldType = FieldType(field_type_id=f[0], name=f[1], description=f[2])
        validatorDb.session.merge(fieldType)

    # insert rule severity
    severityList = [
        (1, 'warning', 'warning'),
        (2, 'fatal', 'fatal error')
    ]
    for s in severityList:
        ruleSeverity = RuleSeverity(rule_severity_id=s[0], name=s[1], description=s[2])
        validatorDb.session.merge(ruleSeverity)

    validatorDb.session.commit()
    validatorDb.session.close()

if __name__ == '__main__':
    setupValidationDB()
