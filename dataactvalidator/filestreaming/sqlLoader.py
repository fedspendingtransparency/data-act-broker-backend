import csv
import os

from dataactcore.config import CONFIG_BROKER
from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.lookups import FILE_TYPE_DICT, RULE_SEVERITY_DICT
from dataactcore.models.validationModels import RuleSql
from dataactvalidator.health_check import create_app
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner


class SQLLoader:
    sql_rules_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "sqlrules")
    headers = ['rule_label', 'rule_error_message', 'rule_cross_file_flag',
               'file_type', 'severity_name', 'query_name', 'target_file']

    @classmethod
    def read_sql_str(cls, filename):
        """Read and clean lines from a .sql file"""
        full_path = os.path.join(cls.sql_rules_path, filename + ".sql")
        with open(full_path, 'rU') as f:
            return f.read()

    @classmethod
    def load_sql(cls, filename):
        """Load SQL-based validation rules to db."""
        with create_app().app_context():
            sess = GlobalDB.db().session

            # Delete all records currently in table
            sess.query(RuleSql).delete()

            filename = os.path.join(cls.sql_rules_path, filename)

            # open csv
            with open(filename, 'rU') as csvfile:
                # read header
                header = csvfile.readline()
                # split header into filed names
                raw_field_names = header.split(',')
                field_names = []
                # clean field names
                for field in raw_field_names:
                    field_names.append(FieldCleaner.clean_string(field))

                unknown_fields = set(field_names) - set(cls.headers)
                if len(unknown_fields) != 0:
                    raise KeyError("".join(["Found unexpected fields: ", str(list(unknown_fields))]))

                missing_fields = set(cls.headers) - set(field_names)
                if len(missing_fields) != 0:
                    raise ValueError("".join(["Missing required fields: ", str(list(missing_fields))]))

                reader = csv.DictReader(csvfile, fieldnames=field_names)
                for row in reader:
                    sql = cls.read_sql_str(row['query_name'])

                    rule_sql = RuleSql(rule_sql=sql, rule_label=row['rule_label'],
                                       rule_error_message=row['rule_error_message'], query_name=row['query_name'])

                    # look up file type id
                    try:
                        file_id = FILE_TYPE_DICT[row["file_type"]]
                    except Exception as e:
                        raise Exception("{}: file type={}, rule label={}. Rule not loaded.".format(
                            e, row["file_type"], row["rule_label"]))
                    try:
                        if row["target_file"].strip() == "":
                            # No target file provided
                            target_file_id = None
                        else:
                            target_file_id = FILE_TYPE_DICT[row["target_file"]]
                    except Exception as e:
                        raise Exception("{}: file type={}, rule label={}. Rule not loaded.".format(
                            e, row["target_file"], row["rule_label"]))

                    # set cross file flag
                    flag = FieldCleaner.clean_string(row["rule_cross_file_flag"])
                    if flag in ('true', 't', 'y', 'yes'):
                        cross_file_flag = True
                    else:
                        cross_file_flag = False

                    rule_sql.rule_severity_id = RULE_SEVERITY_DICT[row['severity_name']]
                    rule_sql.file_id = file_id
                    rule_sql.target_file_id = target_file_id
                    rule_sql.rule_cross_file_flag = cross_file_flag

                    sess.merge(rule_sql)
            sess.commit()

if __name__ == '__main__':
    configure_logging()
    SQLLoader.load_sql("sqlRules.csv")
