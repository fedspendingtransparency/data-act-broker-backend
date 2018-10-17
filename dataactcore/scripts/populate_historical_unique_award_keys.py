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


def update_keys(model, model_type, filter_content, update_years, concat_list):
    """ Run through the loop to update award keys for the given table.

        Args:
            model: the table model to be used
            model_type: whether the table being updated is FABS or FPDS
            filter_content: what we want in the base filter
            update_years: an array of years to work with
            concat_list: the concatenation to update with
    """
    base_query = sess.query(model)
    if model_type == 'FPDS':
        base_query = base_query.filter(model.pulled_from == filter_content)
    else:
        if filter_content == 'aggregate':
            base_query = base_query.filter(model.record_type == '1')
        else:
            base_query = base_query.filter(model.record_type != '1')

    logger.info('Populating unique_award_key for {} {} records...'.format(model_type, filter_content))
    logger.info('Starting records before {}...'.format(update_years[0]))
    base_query.filter(func.cast_as_date(model.action_date) < '{}-01-01'.format(years[0])).\
        update({model.unique_award_key: concat_list}, synchronize_session=False)
    sess.commit()

    for year in update_years:
        logger.info('Starting records in {}...'.format(year))
        base_query.filter(func.cast_as_date(model.action_date) >= '{}-01-01'.format(year),
                          func.cast_as_date(model.action_date) <= '{}-12-31'.format(year)). \
            update({model.unique_award_key: concat_list}, synchronize_session=False)
        sess.commit()

    logger.info('Starting records after {}...'.format(years[-1]))
    base_query.filter(func.cast_as_date(model.action_date) > '{}-12-31'.format(years[-1])). \
        update({model.unique_award_key: concat_list}, synchronize_session=False)
    sess.commit()

    logger.info('{} {} records populated.\n'.format(model_type, filter_content))

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
        dap = DetachedAwardProcurement
        update_keys(dap, 'FPDS', 'award', years, func.concat(func.coalesce(dap.piid, '-none-'), '_',
                                                             func.coalesce(dap.agency_id, '-none-'), '_',
                                                             func.coalesce(dap.parent_award_id, '-none-'), '_',
                                                             func.coalesce(dap.referenced_idv_agency_iden, '-none-')))

        # FPDS IDV
        update_keys(dap, 'FPDS', 'IDV', years, func.concat('IDV_', func.coalesce(dap.piid, '-none-'), '_',
                                                           func.coalesce(dap.agency_id, '-none-')))

        # unpublished FABS record type 1
        dafa = DetachedAwardFinancialAssistance
        update_keys(dafa, 'unpublished FABS', 'aggregate', years,
                    func.concat('AGG_', func.coalesce(dafa.uri, '-none-'), '_',
                                func.coalesce(dafa.awarding_sub_tier_agency_c, '-none-')))

        # unpublished FABS record type not 1
        update_keys(dafa, 'unpublished FABS', 'non-aggregated', years,
                    func.concat('NON_', func.coalesce(dafa.fain, '-none-'), '_',
                                func.coalesce(dafa.awarding_sub_tier_agency_c, '-none-')))

        # published FABS record type 1
        pafa = PublishedAwardFinancialAssistance
        update_keys(pafa, 'published FABS', 'aggregate', years,
                    func.concat('AGG_', func.coalesce(pafa.uri, '-none-'), '_',
                                func.coalesce(pafa.awarding_sub_tier_agency_c, '-none-')))

        # published FABS record type not 1
        update_keys(pafa, 'published FABS', 'non-aggregated', years,
                    func.concat('NON_', func.coalesce(pafa.fain, '-none-'), '_',
                                func.coalesce(pafa.awarding_sub_tier_agency_c, '-none-')))

        sess.close()
