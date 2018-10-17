import logging
import datetime

from sqlalchemy.sql import func

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.stagingModels import (DetachedAwardProcurement, DetachedAwardFinancialAssistance,
                                              PublishedAwardFinancialAssistance)
from dataactcore.models.jobModels import Submission  # noqa
from dataactcore.models.userModel import User  # noqa
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        sess = GlobalDB.db().session

        # Make an array of years starting at 2006 and ending at this year (so it can be run at any time)
        this_year = datetime.datetime.now().year
        years = []
        for i in range(2004, this_year+1):
            years.append(str(i))

        # FPDS awards
        logger.info('Populating unique_award_key for FPDS award records...')
        dap = DetachedAwardProcurement
        logger.info('Starting records before {}...'.format(years[0]))
        sess.query(dap).filter(dap.pulled_from == 'award',
                               func.cast_as_date(dap.action_date) < '{}-01-01'.format(years[0])).\
            update({dap.unique_award_key: func.concat(func.coalesce(dap.piid, '-none-'), '_',
                                                      func.coalesce(dap.agency_id, '-none-'), '_',
                                                      func.coalesce(dap.parent_award_id, '-none-'), '_',
                                                      func.coalesce(dap.referenced_idv_agency_iden, '-none-'))},
                   synchronize_session=False)
        sess.commit()
        for year in years:
            logger.info('Starting records in {}...'.format(year))
            sess.query(dap).filter(dap.pulled_from == 'award',
                                   func.cast_as_date(dap.action_date) >= '{}-01-01'.format(year),
                                   func.cast_as_date(dap.action_date) <= '{}-12-31'.format(year)).\
                update({dap.unique_award_key: func.concat(func.coalesce(dap.piid, '-none-'), '_',
                                                          func.coalesce(dap.agency_id, '-none-'), '_',
                                                          func.coalesce(dap.parent_award_id, '-none-'), '_',
                                                          func.coalesce(dap.referenced_idv_agency_iden, '-none-'))},
                       synchronize_session=False)
            sess.commit()

        logger.info('FPDS award records populated.\n')

        # FPDS IDV
        logger.info('Populating unique_award_key for FPDS IDV records...')
        logger.info('Starting records before {}...'.format(years[0]))
        sess.query(dap).filter(dap.pulled_from == 'IDV',
                               func.cast_as_date(dap.action_date) < '{}-01-01'.format(years[0])).\
            update({dap.unique_award_key: func.concat('IDV_', func.coalesce(dap.piid, '-none-'), '_',
                                                      func.coalesce(dap.agency_id, '-none-'))},
                   synchronize_session=False)
        sess.commit()
        for year in years:
            logger.info('Starting records in {}...'.format(year))
            sess.query(dap).filter(dap.pulled_from == 'IDV',
                                   func.cast_as_date(dap.action_date) >= '{}-01-01'.format(year),
                                   func.cast_as_date(dap.action_date) <= '{}-12-31'.format(year)). \
                update({dap.unique_award_key: func.concat('IDV_', func.coalesce(dap.piid, '-none-'), '_',
                                                          func.coalesce(dap.agency_id, '-none-'))},
                       synchronize_session=False)
            sess.commit()
        logger.info('FPDS IDV records populated.\n')

        # unpublished FABS record type 1
        logger.info('Populating unique_award_key for unpublished FABS aggregate records...')
        dafa = DetachedAwardFinancialAssistance
        logger.info('Starting records before {}...'.format(years[0]))
        sess.query(dafa).filter(dafa.record_type == '1',
                                func.cast_as_date(dafa.action_date) < '{}-01-01'.format(years[0])).\
            update({dafa.unique_award_key: func.concat('AGG_', func.coalesce(dafa.uri, '-none-'), '_',
                                                       func.coalesce(dafa.awarding_sub_tier_agency_c, '-none-'))},
                   synchronize_session=False)
        sess.commit()
        for year in years:
            logger.info('Starting records in {}...'.format(year))
            sess.query(dafa).filter(dafa.record_type == '1',
                                    func.cast_as_date(dafa.action_date) >= '{}-01-01'.format(year),
                                    func.cast_as_date(dafa.action_date) <= '{}-12-31'.format(year)). \
                update({dafa.unique_award_key: func.concat('AGG_', func.coalesce(dafa.uri, '-none-'), '_',
                                                           func.coalesce(dafa.awarding_sub_tier_agency_c, '-none-'))},
                       synchronize_session=False)
            sess.commit()
        logger.info('Unpublished FABS aggregate records populated.\n')

        # unpublished FABS record type not 1
        logger.info('Populating unique_award_key for unpublished FABS non-aggregated records...')
        logger.info('Starting records before {}...'.format(years[0]))
        sess.query(dafa).filter(dafa.record_type != '1',
                                func.cast_as_date(dafa.action_date) < '{}-01-01'.format(years[0])).\
            update({dafa.unique_award_key: func.concat('NON_', func.coalesce(dafa.fain, '-none-'), '_',
                                                       func.coalesce(dafa.awarding_sub_tier_agency_c, '-none-'))},
                   synchronize_session=False)
        sess.commit()
        for year in years:
            logger.info('Starting records in {}...'.format(year))
            sess.query(dafa).filter(dafa.record_type != '1',
                                    func.cast_as_date(dafa.action_date) >= '{}-01-01'.format(year),
                                    func.cast_as_date(dafa.action_date) <= '{}-12-31'.format(year)). \
                update({dafa.unique_award_key: func.concat('NON_', func.coalesce(dafa.fain, '-none-'), '_',
                                                           func.coalesce(dafa.awarding_sub_tier_agency_c, '-none-'))},
                       synchronize_session=False)
            sess.commit()
        logger.info('Unpublished FABS non-aggregated records populated.\n')

        # published FABS record type 1
        logger.info('Populating unique_award_key for published FABS aggregate records...')
        pafa = PublishedAwardFinancialAssistance
        logger.info('Starting records before {}...'.format(years[0]))
        sess.query(pafa).filter(pafa.record_type == '1',
                                func.cast_as_date(pafa.action_date) < '{}-01-01'.format(years[0])).\
            update({pafa.unique_award_key: func.concat('AGG_', func.coalesce(pafa.uri, '-none-'), '_',
                                                       func.coalesce(pafa.awarding_sub_tier_agency_c, '-none-'))},
                   synchronize_session=False)
        sess.commit()
        for year in years:
            logger.info('Starting records in {}...'.format(year))
            sess.query(pafa).filter(pafa.record_type == '1',
                                    func.cast_as_date(pafa.action_date) >= '{}-01-01'.format(year),
                                    func.cast_as_date(pafa.action_date) <= '{}-12-31'.format(year)). \
                update({pafa.unique_award_key: func.concat('AGG_', func.coalesce(pafa.uri, '-none-'), '_',
                                                           func.coalesce(pafa.awarding_sub_tier_agency_c, '-none-'))},
                       synchronize_session=False)
            sess.commit()
        logger.info('Published FABS aggregate records populated.\n')

        # published FABS record type not 1
        logger.info('Populating unique_award_key for published FABS non-aggregated records...')
        logger.info('Starting records before {}...'.format(years[0]))
        sess.query(pafa).filter(pafa.record_type != '1',
                                func.cast_as_date(pafa.action_date) < '{}-01-01'.format(years[0])).\
            update({pafa.unique_award_key: func.concat('NON_', func.coalesce(pafa.fain, '-none-'), '_',
                                                       func.coalesce(pafa.awarding_sub_tier_agency_c, '-none-'))},
                   synchronize_session=False)
        sess.commit()
        for year in years:
            logger.info('Starting records in {}...'.format(year))
            sess.query(pafa).filter(pafa.record_type != '1',
                                    func.cast_as_date(pafa.action_date) >= '{}-01-01'.format(year),
                                    func.cast_as_date(pafa.action_date) <= '{}-12-31'.format(year)). \
                update({pafa.unique_award_key: func.concat('NON_', func.coalesce(pafa.fain, '-none-'), '_',
                                                           func.coalesce(pafa.awarding_sub_tier_agency_c, '-none-'))},
                       synchronize_session=False)
            sess.commit()
        logger.info('Published FABS non-aggregated records populated.\n')

        sess.close()
