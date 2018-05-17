import csv
import os

from dataactcore.config import CONFIG_BROKER
from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.lookups import FILE_TYPE_DICT
from dataactcore.models.validationModels import ValidationLabel
from dataactvalidator.health_check import create_app
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner


class LabelLoader:
    validation_labels_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")
    headers = ['label', 'error_message', 'file_type', 'column_name', 'label_type']

    @classmethod
    def load_labels(cls, filename):
        """Load non-SQL-based validation rules to db."""
        with create_app().app_context():
            sess = GlobalDB.db().session

            # Delete all records currently in table
            sess.query(ValidationLabel).delete()

            filename = os.path.join(cls.validation_labels_path, filename)

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
                    validation_label = ValidationLabel(label=row['label'], error_message=row['error_message'],
                                                       column_name=row['column_name'], label_type=row['label_type'])

                    # look up file type id
                    try:
                        file_id = FILE_TYPE_DICT[row["file_type"]]
                    except Exception as e:
                        raise Exception("{}: file type={}, rule label={}. Rule not loaded.".format(
                            e, row["file_type"], row["rule_label"]))

                    validation_label.file_id = file_id

                    sess.merge(validation_label)
            sess.commit()

if __name__ == '__main__':
    configure_logging()
    LabelLoader.load_labels("validationLabels.csv")
