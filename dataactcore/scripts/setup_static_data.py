from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models import lookups
from dataactcore.models.domainModels import ExternalDataType
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactvalidator.health_check import create_app


def setup_static_data():
    """Create job tracker tables from model metadata."""
    with create_app().app_context():
        sess = GlobalDB.db().session
        insert_codes(sess)
        sess.commit()


def insert_codes(sess):
    """Create static data tables"""

    # insert application types
    for e in lookups.EXTERNAL_DATA_TYPE:
        external_data_type = ExternalDataType(external_data_type_id=e.id, name=e.name, description=e.desc)
        sess.merge(external_data_type)


if __name__ == '__main__':
    configure_logging()
    setup_static_data()
