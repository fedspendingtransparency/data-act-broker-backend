import logging

from sqlalchemy.sql import func

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.stagingModels import (DetachedAwardProcurement, DetachedAwardFinancialAssistance,
                                              PublishedAwardFinancialAssistance)
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        sess = GlobalDB.db().session

        logger.info('Populating unique_award_key for FPDS award records...')
        dap = DetachedAwardProcurement
        sess.query(dap).filter(dap.pulled_from == 'award').\
            update({dap.unique_award_key: func.concat(func.coalesce(dap.piid, '-none-'), '_',
                                                      func.coalesce(dap.agency_id, '-none-'), '_',
                                                      func.coalesce(dap.parent_award_id, '-none-'), '_',
                                                      func.coalesce(dap.referenced_idv_agency_iden, '-none-'))},
                   synchronize_session=False)
        logger.info('FPDS award records populated.\n')

        logger.info('Populating unique_award_key for FPDS IDV records...')
        sess.query(dap).filter(dap.pulled_from == 'IDV').\
            update({dap.unique_award_key: func.concat('IDV_', func.coalesce(dap.piid, '-none-'), '_',
                                                      func.coalesce(dap.agency_id, '-none-'))},
                   synchronize_session=False)
        logger.info('FPDS IDV records populated.\n')

        logger.info('Populating unique_award_key for unpublished FABS aggregate records...')
        dafa = DetachedAwardFinancialAssistance
        sess.query(dafa).filter(dafa.record_type == '1').\
            update({dafa.unique_award_key: func.concat('AGG_', func.coalesce(dafa.uri, '-none-'), '_',
                                                       func.coalesce(dafa.awarding_sub_tier_agency_c, '-none-'))},
                   synchronize_session=False)
        logger.info('Unpublished FABS aggregate records populated.\n')

        logger.info('Populating unique_award_key for unpublished FPDS non-aggregated records...')
        sess.query(dafa).filter(dafa.record_type != '1').\
            update({dafa.unique_award_key: func.concat('NON_', func.coalesce(dafa.fain, '-none-'), '_',
                                                       func.coalesce(dafa.awarding_sub_tier_agency_c, '-none-'))},
                   synchronize_session=False)
        logger.info('Unpublished FABS non-aggregated records populated.\n')

        logger.info('Populating unique_award_key for published FABS aggregate records...')
        pafa = PublishedAwardFinancialAssistance
        sess.query(pafa).filter(pafa.record_type == '1').\
            update({pafa.unique_award_key: func.concat('AGG_', func.coalesce(pafa.uri, '-none-'), '_',
                                                       func.coalesce(pafa.awarding_sub_tier_agency_c, '-none-'))},
                   synchronize_session=False)
        logger.info('Published FABS aggregate records populated.\n')

        logger.info('Populating unique_award_key for published FPDS non-aggregated records...')
        sess.query(pafa).filter(pafa.record_type != '1').\
            update({pafa.unique_award_key: func.concat('NON_', func.coalesce(pafa.fain, '-none-'), '_',
                                                       func.coalesce(pafa.awarding_sub_tier_agency_c, '-none-'))},
                   synchronize_session=False)
        logger.info('Published FABS non-aggregated records populated.\n')

        sess.close()
