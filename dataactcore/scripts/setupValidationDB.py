from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models import lookups
from dataactcore.models.validationModels import FieldType, RuleSeverity
from dataactvalidator.app import createApp


def setupValidationDB():
    """Create validation tables from model metadata and do initial inserts."""
    with createApp().app_context():
        sess = GlobalDB.db().session
        insertCodes(sess)
        sess.commit()


def insertCodes(sess):
    """Insert static data."""

    # insert field types
    for f in lookups.FIELD_TYPE:
        fieldType = FieldType(field_type_id=f.id, name=f.name, description=f.desc)
        sess.merge(fieldType)

    # insert rule severity
    for s in lookups.RULE_SEVERITY:
        ruleSeverity = RuleSeverity(rule_severity_id=s.id, name=s.name, description=s.desc)
        sess.merge(ruleSeverity)


if __name__ == '__main__':
    configure_logging()
    setupValidationDB()
