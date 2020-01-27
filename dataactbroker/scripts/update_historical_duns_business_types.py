import logging
from dataactvalidator.health_check import create_app
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import HistoricDUNS
from dataactcore.models.lookups import DUNS_BUSINESS_TYPE_DICT

from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa

logger = logging.getLogger(__name__)


def main():
    """ One time script to simply update the business types in Historic Duns instead of reloading from the source """
    sess = GlobalDB.db().session

    for historic_duns in sess.query(HistoricDUNS).all():
        historic_duns.business_types = [DUNS_BUSINESS_TYPE_DICT[type_code]
                                        for type_code in historic_duns.business_types_codes
                                        if type_code in DUNS_BUSINESS_TYPE_DICT]
    sess.commit()
    sess.close()
    logger.info("Updating historical DUNS complete")


if __name__ == '__main__':

    with create_app().app_context():
        configure_logging()

        with create_app().app_context():
            main()
