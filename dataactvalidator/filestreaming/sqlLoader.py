import csv
import os
from dataactcore.models.validationModels import RuleSql
from dataactcore.models.validationInterface import ValidationInterface
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactcore.config import CONFIG_BROKER

class SQLLoader():

    sql_rules_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "sqlrules")
    headers = ['rule_label', 'rule_description', 'rule_error_message', 'rule_cross_file_flag',
               'file_type', 'severity_name', 'query_name', 'target_file']

    @classmethod
    def readSqlStr(cls, filename):
        """Read and clean lines from a .sql file"""
        lines = []
        full_path = os.path.join(cls.sql_rules_path, filename + ".sql")
        with open(full_path, 'rU') as f:
            for line in f:
                line = line.replace('\n', '')
                line = FieldCleaner.cleanString(line, removeSpaces=False)
                lines.append(line)
        return ' '.join(lines)

    @classmethod
    def loadSql(cls, filename):
        validationDB = ValidationInterface()

        # Delete all records currently in table
        validationDB.session.query(RuleSql).delete()
        validationDB.session.commit()

        filename = os.path.join(cls.sql_rules_path, filename)

        # open csv
        with open(filename, 'rU') as csvfile:
            # read header
            header = csvfile.readline()
            # split header into filed names
            rawFieldNames = header.split(',')
            fieldNames = []
            # clean field names
            for field in rawFieldNames:
                fieldNames.append(FieldCleaner.cleanString(field))

            unknownFields = set(fieldNames)-set(cls.headers)
            if len(unknownFields) != 0:
                raise KeyError("".join(["Found unexpected fields: ", str(list(unknownFields))]))

            missingFields = set(cls.headers)-set(fieldNames)
            if len(missingFields) != 0:
                raise ValueError("".join(["Missing required fields: ", str(list(missingFields))]))

            reader = csv.DictReader(csvfile, fieldnames=fieldNames)
            for row in reader:
                sql = cls.readSqlStr(row['query_name'])

                rule_sql = RuleSql(rule_sql=sql, rule_label=row['rule_label'], rule_description=row['rule_description'],
                                   rule_error_message=row['rule_error_message'], query_name=row['query_name'])

                # look up file type id
                try:
                    fileId = validationDB.getFileTypeIdByName(FieldCleaner.cleanString(row["file_type"]))
                except Exception as e:
                    raise Exception("{}: file type={}, rule label={}. Rule not loaded.".format(
                        e, row["file_type"], row["rule_label"]))
                try:
                    if row["target_file"].strip() == "":
                        # No target file provided
                        targetFileId = None
                    else:
                        targetFileId =  validationDB.getFileTypeIdByName(FieldCleaner.cleanString(row["target_file"]))
                except Exception as e:
                    raise Exception("{}: file type={}, rule label={}. Rule not loaded.".format(
                        e, row["target_file"], row["rule_label"]))

                # set cross file flag
                if (FieldCleaner.cleanString(row["rule_cross_file_flag"])
                    in ['true', 't', 'y', 'yes']):
                    cross_file_flag = True
                else:
                    cross_file_flag = False

                rule_sql.rule_severity_id = validationDB.getRuleSeverityId(row['severity_name'])
                rule_sql.file_id = fileId
                rule_sql.target_file_id = targetFileId
                rule_sql.rule_cross_file_flag = cross_file_flag

                validationDB.session.merge(rule_sql)
            validationDB.session.commit()

if __name__ == '__main__':
    SQLLoader.loadSql("sqlRules.csv")
