from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models import lookups
from dataactcore.models.validationModels import FieldType, RuleSeverity
from dataactvalidator.health_check import create_app


def setup_validation_db():
    """Create validation tables from model metadata and do initial inserts."""
    with create_app().app_context():
        sess = GlobalDB.db().session
        insert_codes(sess)
        sess.commit()


def insert_codes(sess):
    """Insert static data."""

    # insert field types
    for f in lookups.FIELD_TYPE:
        field_type = FieldType(field_type_id=f.id, name=f.name, description=f.desc)
        sess.merge(field_type)

    # insert rule severity
    for s in lookups.RULE_SEVERITY:
        rule_severity = RuleSeverity(rule_severity_id=s.id, name=s.name, description=s.desc)
        sess.merge(rule_severity)


if __name__ == '__main__':
    configure_logging()
    setup_validation_db()
