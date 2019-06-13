import argparse
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


def update_keys(model, model_type, key_type, filter_content, update_years, concat_list):
    """ Run through the loop to update award keys for the given table.

        Args:
            model: the table model to be used
            model_type: whether the table being updated is FABS or FPDS
            key_type: the type of key (award or transaction) being populated
            filter_content: what we want in the base filter
            update_years: an array of years to work with
            concat_list: the concatenation to update with
    """
    # Uppercasing here for consistency
    if key_type == 'award':
        concat_list = func.upper(concat_list)
    base_query = sess.query(model)
    update_key = model.unique_award_key
    if model_type == 'FPDS':
        base_query = base_query.filter(model.pulled_from == filter_content)
    else:
        if filter_content == 'aggregate':
            base_query = base_query.filter(model.record_type == '1')
        elif filter_content == 'non-aggregate':
            base_query = base_query.filter(model.record_type != '1')

        if key_type == 'transaction':
            update_key = model.afa_generated_unique

    logger.info('Populating unique_{}_key for {} {} records...'.format(key_type, model_type, filter_content))
    if update_years:
        logger.info('Starting records before {}...'.format(update_years[0]))
        base_query.filter(func.cast_as_date(model.action_date) < '{}-01-01'.format(years[0])).\
            update({update_key: concat_list}, synchronize_session=False)
        sess.commit()

        for year in update_years:
            logger.info('Starting records in {}...'.format(year))
            base_query.filter(func.cast_as_date(model.action_date) >= '{}-01-01'.format(year),
                              func.cast_as_date(model.action_date) <= '{}-12-31'.format(year)). \
                update({update_key: concat_list}, synchronize_session=False)
            sess.commit()

        logger.info('Starting records after {}...'.format(years[-1]))
        base_query.filter(func.cast_as_date(model.action_date) > '{}-12-31'.format(years[-1])). \
            update({update_key: concat_list}, synchronize_session=False)
        sess.commit()
    else:
        # DetachedAwardFinancialAssistance table may have values that cannot use cast_as_date. The table is smaller
        # than the rest, so we just update the whole thing at once.
        base_query.update({update_key: concat_list}, synchronize_session=False)
        sess.commit()

    logger.info('{} {} records populated.\n'.format(model_type, filter_content))

if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        sess = GlobalDB.db().session
        parser = argparse.ArgumentParser(description='Update the unique_award_key for the specified tables.')
        parser.add_argument('-m', '--models', help='Specify which models to update', nargs='+', type=str)
        parser.add_argument('-t', '--types', help='Specify which types of records to update', nargs='+', type=str)
        parser.add_argument('-kt', '--key_type', help='Which type of key to populate, award or transaction',
                            required=True, choices=['award', 'transaction'])
        args = parser.parse_args()

        models = ['FPDS', 'unpublishedFABS', 'publishedFABS']
        if args.models:
            for model in args.models:
                if model not in models:
                    raise Exception('Models must be one of the following: {}'.format(', '.join(models)))
            models = args.models

        types = ['award', 'IDV', 'AGG', 'NON']
        if args.types:
            for model_type in args.types:
                if model_type not in types:
                    raise Exception('Types must be one of the following: {}'.format(', '.join(types)))
            types = args.types

        key_type = args.key_type

        # Make an array of years starting at 2006 and ending at this year (so it can be run at any time)
        this_year = datetime.datetime.now().year
        years = []
        for i in range(2004, this_year+1):
            years.append(str(i))

        # FPDS
        if 'FPDS' in models:
            dap = DetachedAwardProcurement

            if key_type == 'award':
                # awards
                if 'award' in types:
                    update_keys(dap, 'FPDS', key_type, 'award', years,
                                func.concat('CONT_AWD_',
                                            func.coalesce(dap.piid, '-none-'), '_',
                                            func.coalesce(dap.agency_id, '-none-'), '_',
                                            func.coalesce(dap.parent_award_id, '-none-'), '_',
                                            func.coalesce(dap.referenced_idv_agency_iden, '-none-')))
                # IDV
                if 'IDV' in types:
                    update_keys(dap, 'FPDS', key_type, 'IDV', years,
                                func.concat('CONT_IDV_', func.coalesce(dap.piid, '-none-'),
                                            '_', func.coalesce(dap.agency_id, '-none-')))
            else:
                # transactions
                if 'award' in types:
                    update_keys(dap, 'FPDS', key_type, 'award', years,
                                func.concat(func.coalesce(dap.agency_id, '-none-'), '_',
                                            func.coalesce(dap.referenced_idv_agency_iden, '-none-'), '_',
                                            func.coalesce(dap.piid, '-none-'), '_',
                                            func.coalesce(dap.award_modification_amendme, '-none-'), '_',
                                            func.coalesce(dap.parent_award_id, '-none-'), '_',
                                            func.coalesce(dap.transaction_number, '-none-'), '_',))
                # IDV
                if 'IDV' in types:
                    update_keys(dap, 'FPDS', key_type, 'IDV', years,
                                func.concat(func.coalesce(dap.agency_id, '-none-'), '_-none-_',
                                            func.coalesce(dap.piid, '-none-'), '_',
                                            func.coalesce(dap.award_modification_amendme, '-none-'), '_-none-_-none-'))

        # unpublished FABS
        if 'unpublishedFABS' in models:
            dafa = DetachedAwardFinancialAssistance

            if key_type == 'award':
                # record type 1
                if 'AGG' in types:
                    update_keys(dafa, 'unpublished FABS', key_type, 'aggregate', None,
                                func.concat('ASST_AGG_', func.coalesce(dafa.uri, '-none-'), '_',
                                            func.coalesce(dafa.awarding_sub_tier_agency_c, '-none-')))
                # record type not 1
                if 'NON' in types:
                    update_keys(dafa, 'unpublished FABS', key_type, 'non-aggregate', None,
                                func.concat('ASST_NON_', func.coalesce(dafa.fain, '-none-'), '_',
                                            func.coalesce(dafa.awarding_sub_tier_agency_c, '-none-')))
            else:
                # All transaction keys are the same format for FABS
                update_keys(dafa, 'unpublished FABS', key_type, '', None,
                            func.concat(func.coalesce(dafa.awarding_sub_tier_agency_c, '-none-'), '_',
                                        func.coalesce(dafa.fain, '-none-'), '_',
                                        func.coalesce(dafa.uri, '-none-'), '_',
                                        func.coalesce(dafa.cfda_number, '-none-'), '_',
                                        func.coalesce(dafa.award_modification_amendme, '-none-')))

        # published FABS
        if 'publishedFABS' in models:
            pafa = PublishedAwardFinancialAssistance

            if key_type == 'award':
                # record type 1
                if 'AGG' in types:
                    update_keys(pafa, 'published FABS', key_type, 'aggregate', years,
                                func.concat('ASST_AGG_', func.coalesce(pafa.uri, '-none-'), '_',
                                            func.coalesce(pafa.awarding_sub_tier_agency_c, '-none-')))

                # record type not 1
                if 'NON' in types:
                    update_keys(pafa, 'published FABS', key_type, 'non-aggregate', years,
                                func.concat('ASST_NON_', func.coalesce(pafa.fain, '-none-'), '_',
                                            func.coalesce(pafa.awarding_sub_tier_agency_c, '-none-')))
            else:
                # All transaction keys are the same format for FABS
                update_keys(pafa, 'published FABS', key_type, '', years,
                            func.concat(func.coalesce(pafa.awarding_sub_tier_agency_c, '-none-'), '_',
                                        func.coalesce(pafa.fain, '-none-'), '_',
                                        func.coalesce(pafa.uri, '-none-'), '_',
                                        func.coalesce(pafa.cfda_number, '-none-'), '_',
                                        func.coalesce(pafa.award_modification_amendme, '-none-')))

        sess.close()
