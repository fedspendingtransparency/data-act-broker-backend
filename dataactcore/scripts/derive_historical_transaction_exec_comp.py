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


def update_transactions(sess, hd_table, min_date, date_type='action_date'):
    """ Update FABS and FPDS transactions with historical executive compensation data.

        Arguments:
            sess: database connection
            exec_comp_data: pandas dataframe representing exec comp data
            min_date: min date to filter on
            date_type: type of date to filter on
    """
    if date_type not in ('action_date', 'created_at'):
        raise ValueError('Invalid date type provided: {}'.format(date_type))

    update_sql = """
            UPDATE {update_table}
            SET
                high_comp_officer1_amount = tmp.high_comp_officer1_amount,
                high_comp_officer1_full_na = tmp.high_comp_officer1_full_na,
                high_comp_officer2_amount = tmp.high_comp_officer2_amount,
                high_comp_officer2_full_na = tmp.high_comp_officer2_full_na,
                high_comp_officer3_amount = tmp.high_comp_officer3_amount,
                high_comp_officer3_full_na = tmp.high_comp_officer3_full_na,
                high_comp_officer4_amount = tmp.high_comp_officer4_amount,
                high_comp_officer4_full_na = tmp.high_comp_officer4_full_na,
                high_comp_officer5_amount = tmp.high_comp_officer5_amount,
                high_comp_officer5_full_na = tmp.high_comp_officer5_full_na
            FROM {table_name} AS hd_table
            WHERE {update_table}.awardee_or_recipient_uniqu = hd_table.awardee_or_recipient_uniqu
                AND cast_as_date({update_table}.{date_type}) >= cast_as_date('{min_date}');
        """
    # Update FABS
    logger.info('Updating FABS based on {}, starting at {} {}'.format(hd_table, date_type, min_date))
    sess.execute(update_sql.format(update_table='published_award_financial_assistance', table_name=hd_table,
                                   min_date=min_date, date_type=date_type))

    # Update FPDS
    logger.info('Updating FPDS based on {}, starting at {}'.format(hd_table, min_date))
    sess.execute(update_sql.format(update_table='detached_award_procurement', table_name=hd_table, min_date=min_date,
                                   date_type=date_type))

    sess.commit()


def main():
    logger.info('Starting historical transaction executive compensation backfill.')

    parser = argparse.ArgumentParser(description='Backfill historical executive compensation data for transactions.')
    algorithm = parser.add_mutually_exclusive_group(required=True)
    algorithm.add_argument('-k', '--ssh_key', help='private key used to access the API remotely', required=True)
    algorithm.add_argument('-c', '--created_at', help='min created_at date when directly using the historic duns table',
                           required=True)
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
        update_transactions(sess, 'historic_duns', args.created_at, date_type='created_at')

if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
