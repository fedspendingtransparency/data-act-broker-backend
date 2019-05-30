import logging
import argparse
import re

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.scripts.load_exec_comp import parse_exec_comp_file, create_temp_exec_comp_table
from dataactcore.utils.duns import get_client, REMOTE_SAM_EXEC_COMP_DIR
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def update_transactions(sess, exec_comp_data, file_date):
    """ Update FABS and FPDS transactions with historical executive compensation data.
    
        Arguments:
            sess: database connection
            exec_comp_data: pandas dataframe representing exec comp data
            file_date: date on the monthly file to use as min date
    """

    temp_table_name = 'temp_exec_comp_update'
    logger.info('Num rows before cleanup: {}'.format(len(exec_comp_data.index)))
    pop_exec = exec_comp_data[exec_comp_data.high_comp_officer1_full_na.notnull()]
    logger.info('Num rows after cleanup: {}'.format(len(pop_exec.index)))
    create_temp_exec_comp_table(sess, temp_table_name, pop_exec)

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
            FROM {table_name} AS tmp
            WHERE {update_table}.awardee_or_recipient_uniqu = tmp.awardee_or_recipient_uniqu
                AND cast_as_date({update_table}.action_date) >= cast_as_date('{file_date}');
        """
    # Update FABS
    logger.info('Updating FABS based on {}, starting at {}'.format(temp_table_name, file_date))
    sess.execute(update_sql.format(update_table='published_award_financial_assistance', table_name=temp_table_name,
                                   file_date=file_date))

    # Update FPDS
    logger.info('Updating FPDS based on {}, starting at {}'.format(temp_table_name, file_date))
    sess.execute(update_sql.format(update_table='detached_award_procurement', table_name=temp_table_name,
                                   file_date=file_date))

    logger.info('Dropping {}'.format(temp_table_name))
    sess.execute('DROP TABLE {};'.format(temp_table_name))

    sess.commit()


def main():
    logger.info('Starting historical transaction executive compensation backfill.')

    parser = argparse.ArgumentParser(description='Backfill historical executive compensation data for transactions.')
    parser.add_argument('-k', '--ssh_key', help='private key used to access the API remotely', required=True)
    args = parser.parse_args()

    root_dir = CONFIG_BROKER['d_file_storage_path']
    client = get_client(ssh_key=args.ssh_key)
    sftp = client.open_sftp()
    # dirlist on remote host
    dirlist = sftp.listdir(REMOTE_SAM_EXEC_COMP_DIR)

    sorted_monthly_file_names = sorted([monthly_file for monthly_file in dirlist if re.match('.*MONTHLY_\d+',
                                                                                             monthly_file)])

    sess = GlobalDB.db().session

    # Testing something
    logger.info('Testing with first file, no deleting or updating')

    exec_comp_data = parse_exec_comp_file(sorted_monthly_file_names[0], root_dir, sftp=sftp, ssh_key=args.ssh_key)
    temp_table_name = 'temp_exec_comp_update'
    logger.info('Num rows before cleanup: {}'.format(len(exec_comp_data.index)))
    pop_exec = exec_comp_data[exec_comp_data.high_comp_officer1_full_na.notnull()]
    logger.info('Num rows after cleanup: {}'.format(len(pop_exec.index)))
    create_temp_exec_comp_table(sess, temp_table_name, pop_exec)

    # for monthly_file in sorted_monthly_file_names:
    #     file_date = re.match('.*(\d{8}).*', monthly_file).group(1)
    #
    #     logger.info('Starting {} monthly file'.format(file_date))
    #     exec_comp_data = parse_exec_comp_file(monthly_file, root_dir, sftp=sftp, ssh_key=args.ssh_key)
    #     update_transactions(sess, exec_comp_data, file_date)

if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
