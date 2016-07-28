from dataactcore.scripts.databaseSetup import createDatabase, runMigrations
from dataactcore.config import CONFIG_DB
from dataactcore.models.validationModels import FileType, FieldType, RuleSeverity
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface

def setupValidationDB():
    """Create validation tables from model metadata and do initial inserts."""
    createDatabase(CONFIG_DB['validator_db_name'])
    runMigrations('validation')
    insertCodes()

def insertCodes():
    """Insert static data."""
    validatorDb = ValidatorValidationInterface()

    # insert file types
    fileTypeList = [
        (1, 'appropriations', 'appropriations file',1),
        (2, 'program_activity','program activity and object class file',2),
        (3, 'award_financial', 'award_financial file',3),
        (4, 'award', 'award file',4)
        ]
    for f in fileTypeList:
        fileType = FileType(file_id=f[0], name=f[1], description=f[2], file_order = f[3])
        validatorDb.session.merge(fileType)

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
