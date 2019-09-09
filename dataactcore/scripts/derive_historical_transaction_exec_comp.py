import logging
import argparse
import re

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.utils.duns import get_client, REMOTE_SAM_EXEC_COMP_DIR, parse_exec_comp_file, \
    create_temp_exec_comp_table
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def update_transactions(sess, hd_table, min_date, date_type='action_date', source='both'):
    """ Update FABS and FPDS transactions with historical executive compensation data.

        Arguments:
            sess: database connection
            exec_comp_data: pandas dataframe representing exec comp data
            min_date: min date to filter on
            date_type: type of date to filter on
    """
    if date_type not in ('action_date', 'created_at', 'updated_at'):
        raise ValueError('Invalid date type provided: {}'.format(date_type))
    if source not in ('fabs', 'fpds', 'both'):
        raise ValueError('Invalid source provided: {}'.format(source))

    update_sql = """
            UPDATE {update_table}
            SET
                high_comp_officer1_amount = hd_table.high_comp_officer1_amount,
                high_comp_officer1_full_na = hd_table.high_comp_officer1_full_na,
                high_comp_officer2_amount = hd_table.high_comp_officer2_amount,
                high_comp_officer2_full_na = hd_table.high_comp_officer2_full_na,
                high_comp_officer3_amount = hd_table.high_comp_officer3_amount,
                high_comp_officer3_full_na = hd_table.high_comp_officer3_full_na,
                high_comp_officer4_amount = hd_table.high_comp_officer4_amount,
                high_comp_officer4_full_na = hd_table.high_comp_officer4_full_na,
                high_comp_officer5_amount = hd_table.high_comp_officer5_amount,
                high_comp_officer5_full_na = hd_table.high_comp_officer5_full_na
            FROM {table_name} AS hd_table
            WHERE {update_table}.awardee_or_recipient_uniqu = hd_table.awardee_or_recipient_uniqu
                AND {compare_date} >= cast_as_date('{min_date}')
                AND {update_table}.high_comp_officer1_amount IS NULL;
        """
    if source in ('fabs', 'both'):
        # Update FABS
        logger.info('Updating FABS based on {}, starting with {} {}'.format(hd_table, date_type, min_date))
        compare_date = 'published_award_financial_assistance.{}'.format(date_type)
        if date_type == 'action_date':
            compare_date = 'cast_as_date({})'.format(compare_date)
        sess.execute(update_sql.format(update_table='published_award_financial_assistance', table_name=hd_table,
                                       min_date=min_date, compare_date=compare_date))
    if source in ('fpds', 'both'):
        # Update FPDS
        logger.info('Updating FPDS based on {}, starting with {} {}'.format(hd_table, date_type, min_date))
        compare_date = 'detached_award_procurement.{}'.format(date_type)
        if date_type == 'action_date':
            compare_date = 'cast_as_date({})'.format(compare_date)
        sess.execute(update_sql.format(update_table='detached_award_procurement', table_name=hd_table,
                                       min_date=min_date, compare_date=compare_date))

    sess.commit()


def main():
    logger.info('Starting historical transaction executive compensation backfill.')

    parser = argparse.ArgumentParser(description='Backfill historical executive compensation data for transactions.')
    algorithm = parser.add_mutually_exclusive_group(required=True)
    algorithm.add_argument('-k', '--ssh_key', help='private key used to access the API remotely')
    algorithm.add_argument('-p', '--pulled_since', help='min created_at/updated_at date when directly using the '
                                                        'historic duns table')
    args = parser.parse_args()

    sess = GlobalDB.db().session

    if args.ssh_key:
        root_dir = CONFIG_BROKER['d_file_storage_path']
        # dirlist on remote host
        client = get_client(ssh_key=args.ssh_key)
        sftp = client.open_sftp()
        dirlist = sftp.listdir(REMOTE_SAM_EXEC_COMP_DIR)
        sorted_monthly_file_names = sorted([monthly_file for monthly_file in dirlist if re.match('.*MONTHLY_\d+',
                                                                                                 monthly_file)])
        for monthly_file in sorted_monthly_file_names:
            file_date = re.match('.*(\d{8}).*', monthly_file).group(1)

            logger.info('Starting {} monthly file'.format(file_date))
            exec_comp_data = parse_exec_comp_file(monthly_file, root_dir, sftp=sftp, ssh_key=args.ssh_key)

            temp_table_name = 'temp_exec_comp_update'
            # Only create a table out of the data we might actually need
            pop_exec = exec_comp_data[exec_comp_data.high_comp_officer1_full_na.notnull()]
            create_temp_exec_comp_table(sess, temp_table_name, pop_exec)

            update_transactions(sess, exec_comp_data, file_date, date_type='action_date')

            logger.info('Dropping {}'.format(temp_table_name))
            sess.execute('DROP TABLE {};'.format(temp_table_name))
            sess.commit()
    else:
        update_transactions(sess, 'historic_duns', args.pulled_since, date_type='created_at', source='fabs')
        update_transactions(sess, 'historic_duns', args.pulled_since, date_type='updated_at', source='fpds')

if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
