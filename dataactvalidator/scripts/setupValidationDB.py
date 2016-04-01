from dataactcore.scripts.databaseSetup import createDatabase
from dataactcore.config import CONFIG_DB
from dataactvalidator.models import validationModels
from dataactvalidator.models.validationModels import FileType, RuleType, FieldType, MultiFieldRuleType
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface

def setupValidationDB(hardReset = False):
    """Create validation tables from model metadata and do initial inserts."""
    createDatabase(CONFIG_DB['validator_db_name'])
    validatorDb = ValidatorValidationInterface()
    # TODO: use Alembic for initial db setup
    if hardReset:
        validationModels.Base.metadata.drop_all(validatorDb.engine)
        validationModels.Base.metadata.create_all(validatorDb.engine)

        # insert file types
        fileTypeList = [(1, 'award', 'award file'),
            (2, 'award_financial', 'award_financial file'),
            (3, 'appropriations', 'appropriations file'),
            (4, 'program_activity','program activity and object class file')]
        for f in fileTypeList:
            fileType = FileType(file_id=f[0], name=f[1], description=f[2])
            validatorDb.session.add(fileType)

        # insert rule types
        ruleTypeList = [(1, 'TYPE', 'checks type'),
            (2, 'EQUAL', 'equals operatior '),
            (3, 'NOT EQUAL', 'not equals operator '),
            (4, 'LESS', 'less than operator '),
            (5, 'GREATER', 'greater than operator'),
            (6, 'LENGTH', 'string length'),
            (7, 'IN_SET', 'value must be in set'),
            (8, 'MIN LENGTH', 'length of data must be at least reference value'),
            (9, 'REQUIRED_CONDITIONAL', 'field is required if secondary rule passes')
            ]
        for r in ruleTypeList:
            ruleType = RuleType(rule_type_id=r[0], name=r[1], description=r[2])
            validatorDb.session.add(ruleType)

        # insert field types
        fieldTypeList = [(1, 'INT', 'integer type'),
            (2, 'DECIMAL', 'decimal type '),
            (3, 'BOOLEAN', 'yes/no'),
            (4, 'STRING', 'string type'),
            (5, 'LONG', 'long integer')]
        for f in fieldTypeList:
            fieldType = FieldType(field_type_id=f[0], name=f[1], description=f[2])
            validatorDb.session.add(fieldType)

        # insert multi-field rule types
        mfrTypeList = [(1, 'CAR_MATCH', 'Matching a set of fields against a CAR file')]
        for m in mfrTypeList:
            mfrt = MultiFieldRuleType(
                multi_field_rule_type_id = m[0], name=m[1], description=m[2])
            validatorDb.session.add(mfrt)
    else:
        validationModels.Base.metadata.create_all(validatorDb.engine)

    validatorDb.session.commit()
    validatorDb.session.close()

if __name__ == '__main__':
    setupValidationDB(True)
