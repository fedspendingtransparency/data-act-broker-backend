import csv

class FieldCleaner:
    """ This class takes a field definition file and cleans it, producing a field definition file that can be read by schemaLoader """

    @staticmethod
    def cleanFile(fileIn, fileOut):
        """ Clean input file line by line and create output file """
        done = False
        # Open CSV file for reading each record as a dictionary
        reader = csv.DictReader(open(fileIn,"r"))
        fieldnames = ["fieldname","required","data_type","field_length"]
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
        try:
            record['fieldname'] = FieldCleaner.cleanName(record['fieldname'])
            record['required'] = FieldCleaner.cleanRequired(record['required'])
            record['data_type'] = FieldCleaner.cleanType(record['data_type'])
            record['field_length'] = FieldCleaner.cleanLength(record['field_length'])
            return record
        except Exception as e:
            print("Exception occurred for field " + record['fieldname'])
            raise e


    @staticmethod
    def cleanName(name):
        """ Remove whitespace from name and change to lowercase """
        # Convert to lowercase and remove whitespace on ends
        name = name.lower().strip()
        # Remove braces and parantheses
        name = name.replace("{","").replace("}","").replace("(","").replace(")","")
        # Replace dashes with underscores
        name = name.replace(" - ","_").replace("-","_")
        # Replace spaces with underscores
        name = name.replace(" ","_")
        name = name.replace(",","_")
        name = name.replace("/","_")
        # Remove duplicate underscores
        name = name.replace("__","_")
        return name

    @staticmethod
    def cleanRequired(required):
        """ Convert 'required' and '(required)' to True, "optional" and "required if relevant" if False, otherwise raises an exception """
        required = required.lower().strip()
        if(required == "required" or required == "(required)" or required == "true"):
            return "true"
        elif(required == "false" or required == "" or required == "optional" or required == "required if relevant" or required == "required if modification"):
            return "false"
        else:
            print("Required is " + str(required))
            raise ValueError("Unknown value for required")

    @staticmethod
    def cleanType(type):
        """ Interprets all inputs as int, str, or bool.  For unexpected inputs, raises an exception. """
        type = type.lower().strip()
        if(type == "integer" or type == "int"):
            return "int"
        elif(type == "numeric" or type == "float"):
            return "float"
        elif(type == "alphanumeric" or type == "" or type == "str"):
            return "str"
        elif(type == "alphanumeric (logically a boolean)"):
            # Some of these are intended to be booleans, but others use this value when they have multiple possible values,
            # so for now we have to treat them as strings
            return "str"
        elif(type == "boolean" or type == "bool"):
            return "bool"
        else:
            print("Type is " + str(type))
            raise ValueError("Unknown type")

    @staticmethod
    def cleanLength(length):
        """ Checks that input is a positive integer, otherwise raises an exception. """
        length = length.strip()
        if(length == ""):
            # Empty length is fine, this means there is no length requirement
            return ""
        try:
            int(length)
        except:
            # length cannot be cast as int
            print("Length is type: " + str(type(length)))
            print("Length is " + str(length))
            raise ValueError("Length must be an integer")
        if(length <= 0):
            raise ValueError("Length must be positive")
        return length

if __name__ == '__main__':
    #FieldCleaner.cleanFile("programActivityRaw.csv","programActivityFields.csv")
    #FieldCleaner.cleanFile("awardFinFields.csv","awardFinancialFields.csv")
    FieldCleaner.cleanFile("awardFieldsRaw.csv","awardFields.csv")