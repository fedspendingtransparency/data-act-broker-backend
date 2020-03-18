import logging
import os
import pandas as pd

from dataactcore.config import CONFIG_BROKER
from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.lookups import FILE_TYPE_DICT, RULE_SEVERITY_DICT
from dataactcore.models.validationModels import RuleSql
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data
from dataactvalidator.scripts.loader_utils import insert_dataframe
from dataactbroker.helpers.pandas_helper import check_dataframe_diff

logger = logging.getLogger(__name__)


class SQLLoader:
    sql_rules_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "sqlrules")
    headers = ['rule_label', 'rule_error_message', 'rule_cross_file_flag', 'file_type', 'severity_name', 'query_name',
               'target_file', 'expected_value', 'category']

    @classmethod
    def read_sql_str(cls, filename):
        """ Read and clean lines from a .sql file """
        full_path = os.path.join(cls.sql_rules_path, filename + ".sql")
        with open(full_path, 'rU') as f:
            return f.read()

    @classmethod
    def load_sql(cls, filename):
        """ Load SQL-based validation rules to db. """
        with create_app().app_context():
            sess = GlobalDB.db().session
            filename = os.path.join(cls.sql_rules_path, filename)

            # Initial load
            sql_data = pd.read_csv(filename, dtype=str, usecols=cls.headers)
            sql_data = clean_data(
                sql_data,
                RuleSql,
                {'rule_label': 'rule_label', 'rule_error_message': 'rule_error_message', 'query_name': 'query_name',
                 'expected_value': 'expected_value', 'category': 'category', 'file_type': 'file_type',
                 'target_file': 'target_file', 'rule_cross_file_flag': 'rule_cross_file_flag',
                 'severity_name': 'severity_name'},
                {}
            )

            # Processing certain values
            sql_data['rule_sql'] = sql_data['query_name'].apply(lambda name: cls.read_sql_str(name))
            sql_data['file_id'] = sql_data['file_type'].apply(lambda type: FILE_TYPE_DICT.get(type, None))
            if sql_data['file_id'].isnull().values.any():
                raise Exception('Invalid file_type value found in sqlLoader. Must be one of the following: {}'
                                .format(', '.join(list(FILE_TYPE_DICT.keys()))))
            sql_data['target_file_id'] = sql_data['target_file'].apply(lambda type: FILE_TYPE_DICT.get(type, None))
            sql_data['rule_cross_file_flag'] = sql_data['rule_cross_file_flag'].apply(lambda flag:
                                                                                      flag in ('true', 't', 'y', 'yes'))
            sql_data['rule_severity_id'] = sql_data['severity_name'].apply(lambda severity_name:
                                                                           RULE_SEVERITY_DICT.get(severity_name, None))
            if sql_data['rule_severity_id'].isnull().values.any():
                raise Exception('Invalid severity_name value found in sqlLoader Must be one of the following: {}'
                                .format(', '.join(list(RULE_SEVERITY_DICT.keys()))))
            sql_data.drop(['file_type', 'severity_name', 'target_file'], axis=1, inplace=True)

            # Final check if we need to actually reload
            if check_dataframe_diff(sql_data, RuleSql, del_cols=['rule_sql_id', 'created_at', 'updated_at'],
                                    sort_cols=['rule_label', 'file_id', 'target_file_id']):
                # Delete and reload all records currently in table
                logger.info('Detected changes in {}, deleting RuleSQL and reloading'.format(cls.sql_rules_path))
                sess.query(RuleSql).delete()
                insert_dataframe(sql_data, RuleSql.__table__.name, sess.connection())
                sess.commit()
            else:
                logger.info('No changes detected since last load. Skipping.')

if __name__ == '__main__':
    configure_logging()
    SQLLoader.load_sql("sqlRules.csv")
