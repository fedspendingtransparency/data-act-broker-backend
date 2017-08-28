import csv

from dataactcore.logging import configure_logging
from dataactcore.utils.stringCleaner import StringCleaner
from dataactcore.models.lookups import FIELD_TYPE_DICT_ID


class FieldCleaner(StringCleaner):
    """ This class takes a field definition file and cleans it,
        producing a field definition file that can be read by schemaLoader """

    @staticmethod
    def clean_file(file_in, file_out):
        """ Clean input file line by line and create output file """
        # Open CSV file for reading each record as a dictionary
        with open(file_in, "rU") as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = ["fieldname", "fieldname_short", "required", "data_type", "field_length", "rule_labels"]
            writer = csv.DictWriter(open(file_out, "w"), fieldnames=fieldnames, lineterminator='\n')
            writer.writeheader()
            for record in reader:
                # Pass record into clean_record to sanitize
                record = FieldCleaner.clean_record(record)
                # Write new row to output file
                writer.writerow(record)

    @staticmethod
    def clean_record(record):
        """ Clean up an individual record, and write to output file.

        Args:
            record: dict of field specifications, keys should be 'fieldname','required','data_type', and 'field_length'

        Returns:
            Cleaned up version of record dict with same keys
        """

        # Clean name, required, type, and length
        record['fieldname'] = FieldCleaner.clean_name(record['fieldname'])
        # fieldname_short is the machine-friendly name provided with the
        # schema, so the only change we'll make to it is stripping whitespace
        record['fieldname_short'] = record['fieldname_short'].strip()
        record['required'] = FieldCleaner.clean_required(record['required'])
        record['data_type'] = FieldCleaner.clean_type(record['data_type'])
        record['field_length'] = FieldCleaner.clean_length(record['field_length'])
        return record

    @staticmethod
    def clean_name(name):
        """ Remove whitespace from name and change to lowercase, also clean up special characters """
        # Convert to lowercase and remove whitespace on ends
        name = FieldCleaner.clean_string(name)
        # Remove braces and parentheses
        name = name.replace("{", "").replace("}", "").replace("(", "").replace(")", "")
        # Replace problematic characters with underscores
        name = name.replace(" - ", "_").replace("-", "_")
        name = name.replace(",", "_")
        name = name.replace("/", "_")
        # Remove duplicate underscores
        name = name.replace("__", "_")
        return name

    @staticmethod
    def clean_required(required):
        """ Convert 'required' and '(required)' to True, "optional" and "required if relevant" if False,
            otherwise raises an exception """
        required = FieldCleaner.clean_string(required, False)
        if required[0:3].lower() == "asp":
            # Remove ASP prefix
            required = required[5:]
        if required == "required" or required == "(required)" or required == "true":
            return "true"
        elif required == "false" or required == "" or required == "optional" or required == "required if relevant" or \
                required == "required if modification" or required == "conditional per validation rule" or \
                required == "conditional per award type" or required == "conditionally required" or \
                required == "derived":
            return "false"
        else:
            raise ValueError("".join(["Unknown value for required: ", required]))

    @staticmethod
    def clean_type(clean_type):
        """ Interprets all inputs as int, str, or bool.  For unexpected inputs, raises an exception. """
        clean_type = FieldCleaner.clean_string(clean_type, False)
        if clean_type == "integer" or clean_type == "int":
            return "int"
        elif clean_type == "numeric" or clean_type == "float":
            return "float"
        elif clean_type == "alphanumeric" or clean_type == "" or clean_type == "str" or clean_type == "string":
            return "str"
        elif clean_type == "alphanumeric (logically a boolean)":
            # Some of these are intended to be booleans,
            # but others use this value when they have multiple possible values,
            # so for now we have to treat them as strings
            return "str"
        elif clean_type == "boolean" or clean_type == "bool":
            return "bool"
        elif clean_type == "long":
            return "long"
        else:
            raise ValueError("".join(["Unknown type: ", clean_type]))

    @staticmethod
    def clean_length(length):
        """ Checks that input is a positive integer, otherwise raises an exception. """
        length = FieldCleaner.clean_string(length, False)
        if length == "":
            # Empty length is fine, this means there is no length requirement
            return ""
        try:
            int(length)
        except:
            # length cannot be cast as int
            raise ValueError("Length must be an integer")
        if int(length) <= 0:
            raise ValueError("Length must be positive")
        return length

    @classmethod
    def clean_row(cls, row, long_to_short_dict, fields):
        """ Strips whitespace, replaces empty strings with None, and pads fields that need it

        Args:
            row: Record in this row
            long_to_short_dict: Maps long column names to short
            fields: List of FileColumn objects for this file type

        Returns:
            Cleaned row
        """

        for field in fields:
            key = long_to_short_dict[field.name]
            field_type = FIELD_TYPE_DICT_ID[field.field_types_id]
            value = row[key]
            if value is not None:
                # Remove extra whitespace
                value = value.strip()
                # If field is string and has triple quotes, remove. Single quote because csv converts triple to single
                if field_type in ["STRING"] and value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                if field_type in ["INT", "DECIMAL", "LONG"]:
                    temp_value = value.replace(",", "")
                    if FieldCleaner.is_numeric(temp_value):
                        value = temp_value
                if value == "":
                    # Replace empty strings with null
                    value = None

                row[key] = cls.pad_field(field, value)
        return row

    @staticmethod
    def pad_field(field, value):
        """ Pad value with appropriate number of leading zeros if needed

        Args:
            field: FileColumn object
            value: Value present in row

        Returns:
            Padded value
        """
        # Check padded flag for this field and file
        if value is not None and field.padded_flag and field.length is not None:
            # Pad to specified length with leading zeros
            return value.zfill(field.length)
        else:
            return value

if __name__ == '__main__':
    configure_logging()
    FieldCleaner.clean_file("../config/awardProcurementFieldsRaw.csv", "../config/awardProcurementFields.csv")
    FieldCleaner.clean_file("../config/appropFieldsRaw.csv", "../config/appropFields.csv")
    FieldCleaner.clean_file("../config/awardFinancialFieldsRaw.csv", "../config/awardFinancialFields.csv")
    FieldCleaner.clean_file("../config/programActivityFieldsRaw.csv", "../config/programActivityFields.csv")
    FieldCleaner.clean_file("../config/awardFieldsRaw.csv", "../config/awardFields.csv")
    FieldCleaner.clean_file("../config/detachedAwardFieldsRaw.csv", "../config/detachedAwardFields.csv")
