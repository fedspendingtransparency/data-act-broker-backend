import csv
import os
import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import FileType
from dataactcore.models.validationModels import FileColumn, FieldType
from dataactvalidator.health_check import create_app
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner

logger = logging.getLogger(__name__)


class SchemaLoader(object):
    """Load schema from corresponding .csv and insert related validation rules to the db."""
    # TODO: add schema to .csv mapping to the db instead of hard-coding here.
    fieldFiles = {
        "appropriations": "appropFields.csv",
        "award": "awardFields.csv",
        "award_financial": "awardFinancialFields.csv",
        "program_activity": "programActivityFields.csv",
        "award_procurement": "awardProcurementFields.csv",
        "detached_award": "fabsFields.csv",
        "executive_compensation": "executiveCompensationFields.csv"}

    @staticmethod
    def load_fields(file_type_name, schema_file_name):
        """Load specified schema from a .csv."""
        with create_app().app_context():
            sess = GlobalDB.db().session

            # get file type object for specified fileTypeName
            file_type = sess.query(FileType).filter(FileType.name == file_type_name).one()

            # delete existing schema from database
            SchemaLoader.remove_columns_by_file_type(sess, file_type)

            # get allowable datatypes
            type_query = sess.query(FieldType.name, FieldType.field_type_id).all()
            types = {data_type.name: data_type.field_type_id for data_type in type_query}

            # add schema to database
            with open(schema_file_name, 'rU') as csvfile:
                reader = csv.DictReader(csvfile)
                file_column_count = 0
                for record in reader:
                    record = FieldCleaner.clean_record(record)

                    fields = ["fieldname", "required", "data_type"]
                    if all(field in record for field in fields):
                        SchemaLoader.add_column_by_file_type(
                            sess,
                            types,
                            file_type,
                            FieldCleaner.clean_string(record["fieldname"]),
                            FieldCleaner.clean_string(record["fieldname_short"]),
                            record["required"],
                            record["data_type"],
                            record["padded_flag"],
                            record["field_length"])
                        file_column_count += 1
                    else:
                            raise ValueError('CSV File does not follow schema')

                sess.commit()
                logger.info({
                    'message': '{} {} schema records added to {}'.format(file_column_count, file_type_name,
                                                                         FileColumn.__tablename__),
                    'message_type': 'ValidatorInfo',
                    'file_type': file_type.letter_name
                })

    @staticmethod
    def remove_columns_by_file_type(sess, file_type):
        """Remove the schema for a specified file type."""
        deleted_records = sess.query(FileColumn).filter(FileColumn.file == file_type).delete(
            synchronize_session='fetch')
        logger.info({
            'message': '{} {} schema records deleted from {}'.format(deleted_records, file_type.name,
                                                                     FileColumn.__tablename__),
            'message_type': 'ValidatorInfo',
            'file_type': file_type.letter_name
        })

    @staticmethod
    def add_column_by_file_type(sess, types, file_type, field_name, field_name_short, required, field_type,
                                padded_flag="False", field_length=None):
        """
        Adds a new column to the schema

        Args:
        file_type -- FileType object this column belongs to
        field_name -- The name of the schema column
        types -- List of field types
        field_name_short -- The machine-friendly, short column name
        required --  marks the column if data is allways required
        field_type  -- sets the type of data allowed in the column
        padded_flag -- True if this column should be padded
        field_length -- Maximum allowed length for this field

        """
        new_column = FileColumn()
        new_column.file = file_type
        new_column.required = False
        new_column.name = field_name.lower().strip().replace(' ', '_')
        new_column.name_short = field_name_short.lower().strip().replace(' ', '_')
        field_type = field_type.upper()

        # Allow for other names
        if field_type == "STR":
            field_type = "STRING"
        elif field_type == "FLOAT":
            field_type = "DECIMAL"
        elif field_type == "BOOL":
            field_type = "BOOLEAN"

        # Translate padded flag to true or false
        if not padded_flag:
            new_column.padded_flag = False
        elif padded_flag.lower() == "true":
            new_column.padded_flag = True
        else:
            new_column.padded_flag = False

        # Check types
        if field_type in types:
            new_column.field_types_id = types[field_type]
        else:
            raise ValueError('Type {} not value for {}'.format(field_type, field_name))

        # Check Required
        if required.lower() in ['true', 'false']:
            if required.lower() == 'true':
                new_column.required = True
        else:
            raise ValueError('Required field is not boolean for {}'.format(field_name))

        # Add length if present
        if field_length is not None and str(field_length).strip() != "":
            length_int = int(str(field_length).strip())
            new_column.length = length_int

        sess.add(new_column)

    @classmethod
    def load_all_from_path(cls, path):
        # Load field definitions into validation DB
        for key in cls.fieldFiles:
            filepath = os.path.join(path, cls.fieldFiles[key])
            cls.load_fields(key, filepath)

if __name__ == '__main__':
    configure_logging()
    SchemaLoader.load_all_from_path("../config/")
