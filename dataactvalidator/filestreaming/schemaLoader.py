import csv
import os
import logging
from dataactcore.interfaces.db import databaseSession
from dataactcore.models.validationModels import FileColumn, FileTypeValidation, FieldType
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SchemaLoader(object):
    """Load schema from corresponding .csv and insert related validation rules to the db."""
    # TODO: add schema to .csv mapping to the db instead of hard-coding here.
    fieldFiles = {
        "appropriations": "appropFields.csv",
        "award": "awardFields.csv",
        "award_financial": "awardFinancialFields.csv",
        "program_activity": "programActivityFields.csv",
        "award_procurement": "awardProcurementFields.csv"}

    @staticmethod
    def loadFields(fileTypeName, schemaFileName):
        """Load specified schema from a .csv."""
        with databaseSession() as sess:

            # get file type object for specified fileTypeName
            fileType = sess.query(FileTypeValidation).filter(FileTypeValidation.name == fileTypeName).one()

            # delete existing schema from database
            SchemaLoader.removeColumnsByFileType(sess, fileType)

            # get allowable datatypes
            typeQuery = sess.query(FieldType.name, FieldType.field_type_id).all()
            types = {type.name: type.field_type_id for type in typeQuery}

            # add schema to database
            with open(schemaFileName, 'rU') as csvfile:
                reader = csv.DictReader(csvfile)
                for record in reader:
                    record = FieldCleaner.cleanRecord(record)

                    fields = ["fieldname", "required", "data_type"]
                    if all(field in record for field in fields):
                        SchemaLoader.addColumnByFileType(
                            sess,
                            types,
                            fileType,
                            FieldCleaner.cleanString(record["fieldname"]),
                            FieldCleaner.cleanString(record["fieldname_short"]),
                            record["required"],
                            record["data_type"],
                            record["padded_flag"],
                            record["field_length"])
                    else:
                            raise ValueError('CSV File does not follow schema')

                sess.commit()

    @staticmethod
    def removeColumnsByFileType(sess, fileType):
        """Remove the schema for a specified file type."""
        deletedRecords = sess.query(FileColumn).filter(FileColumn.file == fileType).delete(
            synchronize_session='fetch')
        logger.info('{} {} schema records deleted from {}'.format(
            deletedRecords, fileType.name, FileColumn.__tablename__))

    @staticmethod
    def addColumnByFileType(sess, types, fileType, fieldName, fieldNameShort, required, field_type, paddedFlag="False",
                            fieldLength=None):
        """
        Adds a new column to the schema

        Args:
        fileType -- FileTypeValidation object this column belongs to
        fieldName -- The name of the schema column
        types -- List of field types
        fieldNameShort -- The machine-friendly, short column name
        required --  marks the column if data is allways required
        field_type  -- sets the type of data allowed in the column
        paddedFlag -- True if this column should be padded
        fieldLength -- Maximum allowed length for this field

        """
        newColumn = FileColumn()
        newColumn.file = fileType
        newColumn.required = False
        newColumn.name = fieldName
        newColumn.name_short = fieldNameShort
        field_type = field_type.upper()

        # Allow for other names
        if field_type == "STR":
            field_type = "STRING"
        elif field_type == "FLOAT":
            field_type = "DECIMAL"
        elif field_type == "BOOL":
            field_type = "BOOLEAN"

        # Translate padded flag to true or false
        if not paddedFlag:
            newColumn.padded_flag = False
        elif paddedFlag.lower() == "true":
            newColumn.padded_flag = True
        else:
            newColumn.padded_flag = False

        # Check types
        if field_type in types:
            newColumn.field_types_id = types[field_type]
        else:
            raise ValueError('Type {} not value for {}'.format(field_type, fieldName))

        # Check Required
        if required.lower() in ['true', 'false']:
            if required.lower() == 'true':
                newColumn.required = True
        else:
            raise ValueError('Required field is not boolean for {}'.format(fieldName))

        # Add length if present
        if fieldLength is not None and str(fieldLength).strip() != "":
            lengthInt = int(str(fieldLength).strip())
            newColumn.length = lengthInt

        sess.add(newColumn)

    @classmethod
    def loadAllFromPath(cls,path):
        # Load field definitions into validation DB
        for key in cls.fieldFiles:
            filepath = os.path.join(path,cls.fieldFiles[key])
            cls.loadFields(key,filepath)

if __name__ == '__main__':
    SchemaLoader.loadAllFromPath("../config/")
