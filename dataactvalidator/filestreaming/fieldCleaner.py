import csv

from dataactcore.logging import configure_logging
from dataactcore.utils.stringCleaner import StringCleaner


class FieldCleaner(StringCleaner):
    """ This class takes a field definition file and cleans it, producing a field definition file that can be read by schemaLoader """

    @staticmethod
    def cleanFile(fileIn, fileOut):
        """ Clean input file line by line and create output file """
        done = False
        # Open CSV file for reading each record as a dictionary
        with open(fileIn, "rU") as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = ["fieldname","fieldname_short","required","data_type","field_length","rule_labels"]
            writer = csv.DictWriter(open(fileOut,"w"),fieldnames=fieldnames,lineterminator='\n')
            writer.writeheader()
            for record in reader:
                # Pass record into cleanRecord to sanitize
                record = FieldCleaner.cleanRecord(record)
                # Write new row to output file
                writer.writerow(record)

    @staticmethod
    def cleanRecord(record):
        """ Clean up an individual record, and write to output file.

        Args:
            record: dict of field specifications, keys should be 'fieldname','required','data_type', and 'field_length'

        Returns:
            Cleaned up version of record dict with same keys
        """

        # Clean name, required, type, and length
        record['fieldname'] = FieldCleaner.cleanName(record['fieldname'])
        # fieldname_short is the machine-friendly name provided with the
        # schema, so the only change we'll make to it is stripping whitespace
        record['fieldname_short'] = record['fieldname_short'].strip()
        record['required'] = FieldCleaner.cleanRequired(record['required'])
        record['data_type'] = FieldCleaner.cleanType(record['data_type'])
        record['field_length'] = FieldCleaner.cleanLength(record['field_length'])
        return record

    @staticmethod
    def cleanName(name):
        """ Remove whitespace from name and change to lowercase, also clean up special characters """
        # Convert to lowercase and remove whitespace on ends
        originalName = name
        name = FieldCleaner.cleanString(name)
        # Remove braces and parantheses
        name = name.replace("{","").replace("}","").replace("(","").replace(")","")
        # Replace problematic characters with underscores
        name = name.replace(" - ","_").replace("-","_")
        name = name.replace(",","_")
        name = name.replace("/","_")
        # Remove duplicate underscores
        name = name.replace("__","_")
        return name

    @staticmethod
    def cleanRequired(required):
        """ Convert 'required' and '(required)' to True, "optional" and "required if relevant" if False, otherwise raises an exception """
        required = FieldCleaner.cleanString(required,False)
        if required[0:3].lower() == "asp":
            # Remove ASP prefix
            required = required[5:]
        if(required == "required" or required == "(required)" or required == "true"):
            return "true"
        elif(required == "false" or required == "" or required == "optional" or required == "required if relevant" or required == "required if modification" or required == "conditional per validation rule" or required == "conditional per award type" or required == "conditionally required" or required == "derived"):
            return "false"
        else:
            raise ValueError("".join(["Unknown value for required: ", required]))

    @staticmethod
    def cleanType(type):
        """ Interprets all inputs as int, str, or bool.  For unexpected inputs, raises an exception. """
        type = FieldCleaner.cleanString(type,False)
        if(type == "integer" or type == "int"):
            return "int"
        elif(type == "numeric" or type == "float"):
            return "float"
        elif(type == "alphanumeric" or type == "" or type == "str" or type == "string"):
            return "str"
        elif(type == "alphanumeric (logically a boolean)"):
            # Some of these are intended to be booleans, but others use this value when they have multiple possible values,
            # so for now we have to treat them as strings
            return "str"
        elif(type == "boolean" or type == "bool"):
            return "bool"
        elif(type == "long"):
            return "long"
        else:
            raise ValueError("".join(["Unknown type: ", type]))

    @staticmethod
    def cleanLength(length):
        """ Checks that input is a positive integer, otherwise raises an exception. """
        length = FieldCleaner.cleanString(length,False)
        if(length == ""):
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
    def cleanRow(cls, row, longToShortDict, fields):
        """ Strips whitespace, replaces empty strings with None, and pads fields that need it

        Args:
            row: Record in this row
            longToShortDict: Maps long column names to short
            fields: List of FileColumn objects for this file type

        Returns:
            Cleaned row
        """

        for field in fields:
            key = longToShortDict[field.name]
            field_type = field.field_type.name
            value = row[key]
            if value is not None:
                # Remove extra whitespace
                value = value.strip()
                if field_type in ["INT", "DECIMAL", "LONG"]:
                    tempValue = value.replace(",","")
                    if FieldCleaner.isNumeric(tempValue):
                        value = tempValue
                if value == "":
                    # Replace empty strings with null
                    value = None

                row[key] = cls.padField(field,value)
        return row

    @staticmethod
    def padField(field, value):
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
    FieldCleaner.cleanFile("../config/awardProcurementFieldsRaw.csv","../config/awardProcurementFields.csv")
    FieldCleaner.cleanFile("../config/appropFieldsRaw.csv","../config/appropFields.csv")
    FieldCleaner.cleanFile("../config/awardFinancialFieldsRaw.csv","../config/awardFinancialFields.csv")
    FieldCleaner.cleanFile("../config/programActivityFieldsRaw.csv","../config/programActivityFields.csv")
    FieldCleaner.cleanFile("../config/awardFieldsRaw.csv","../config/awardFields.csv")
