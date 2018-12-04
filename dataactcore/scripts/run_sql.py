import argparse
import logging

from dataactcore.interfaces.db import GlobalDB
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    """ Parse a SQL file and run it against the database

        Params:
            -f: List of files to run, one at a time, in the order given.
    """
    parser = argparse.ArgumentParser(description='Runs SQL directly, pulled from a file.')
    parser.add_argument('-f', '--files', action="append", nargs="+", help='List of space-separated SQL filepaths.')
    args = parser.parse_args()

    if not args.files or len(args.files) < 1:
        raise Exception('At least one filepath to a SQL file must be included.')

    with create_app().app_context():
        for filepath in args.files[0]:
            with open(filepath, 'r') as file:
                sql = file.read()
                logger.info('Running SQL:')
                logger.info(sql)

                sql = sql.replace('\n', ' ')
                sess = GlobalDB.db().session
                sess.execute(sql)
                sess.commit()

                logger.info('SQL completed\n')
