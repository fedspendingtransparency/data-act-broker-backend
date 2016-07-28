import csv
from sqlalchemy.exc import IntegrityError
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactvalidator.validation_handlers.validator import Validator

class LoaderUtils:

    @staticmethod
    def checkRecord (record, fields) :
        """ Returns True if all elements of fields are present in record """
        for data in fields:
            if ( not data in record ):
                return False
        return True

    @staticmethod
    def compareRecords (recordA,recordB, fields) :
        """ Compares two dictionaries based of a field subset """
        for data in fields:
            if (  data in recordA and  data in recordB   ):
                if( not recordA[data]== recordB[data]) :
                    return False
            else :
                return False
        return True

    @classmethod
    def loadCsv(cls,filename,model,interface,fieldMap,fieldOptions):
        """ Loads a table based on a csv

        Args:
            filename: CSV to load
            model: ORM object for table to be loaded
            interface: interface to DB table is in
            fieldMap: dict that maps columns of the csv to attributes of the ORM object
            fieldOptions: dict with keys of attribute names, value contains a dict with options for that attribute.
                Current options are "pad_to_length" which if present will pad the field with leading zeros up to
                specified length, and "skip_duplicate" which ignores subsequent lines that repeat values.
        """
        # Delete all records currently in table
        interface.session.query(model).delete()
        interface.session.commit()
        valuePresent = {}
        # Open csv
        with open(filename,'rU') as csvfile:
            # Read header
            header = csvfile.readline()
            # Split header into fieldnames
            rawFieldNames = header.split(",")
            fieldNames = []
            # Clean field names
            for field in rawFieldNames:
                fieldNames.append(FieldCleaner.cleanString(field))
            # Map fieldnames to attribute names
            attributeNames = []
            for field in fieldNames:
                if field in fieldMap:
                    attributeNames.append(fieldMap[field])
                    if fieldMap[field] in fieldOptions and "skip_duplicates" in fieldOptions[fieldMap[field]]:
                        # Create empty dict for this field
                        valuePresent[fieldMap[field]] = {}
                else:
                    raise KeyError("".join(["Found unexpected field ", str(field)]))
            # Check that all fields are present
            for field in fieldMap:
                if not field in fieldNames:
                    raise ValueError("".join([str(field)," is required for loading table ", str(type(model))]))
            # Open DictReader with attribute names
            reader = csv.DictReader(csvfile,fieldnames = attributeNames)
            # For each row, create instance of model and add it
            for row in reader:
                skipInsert = False
                for field in fieldOptions:
                    if row[field] is None:
                        # If field is empty set to an empty string
                        row[field] = ""
                    # For each field with options present, modify according to those options
                    options = fieldOptions[field]
                    if "strip_commas" in options:
                        # Remove commas from numeric fields
                        row[field] = row[field].replace(",","")
                        if row[field] == "":
                            # If empty, set to 0
                            row[field] = "0"
                    if "pad_to_length" in options:
                        padLength = options["pad_to_length"]
                        row[field] = Validator.padToLength(row[field],padLength)
                    if "skip_duplicates" in options:
                        if row[field] is None or len(row[field].strip()) == 0 or row[field] in valuePresent[field]:
                            # Value not provided or already exists, skip it
                            skipInsert = True
                        else:
                            # Insert new value
                            valuePresent[field][row[field]] = True
                record = model(**row)
                if not skipInsert:
                    try:
                        interface.session.merge(record)
                        interface.session.commit()

                    except IntegrityError as e:
                        # Hit a duplicate value that violates index, skip this one
                        print("".join(["Warning: Skipping this row: ",str(row)]))
                        print("".join(["Due to error: ",str(e)]))
                        interface.session.rollback()
                        continue
            interface.session.commit()


