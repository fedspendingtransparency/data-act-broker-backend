from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactvalidator.validation_handlers.validator import Validator
from datetime import datetime

class LoaderUtils:

    # define some data-munging functions that can be applied to
    # pandas dataframes as necessary
    # padFunction = lambda field, padTo: str(field).strip().zfill(padTo)
    currentTimeFunction = lambda x: datetime.utcnow()
    cleanColNamesFunction = lambda field: str(field).lower().strip().replace(" ","_").replace(",","_")

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
    def cleanData(cls, data, model, fieldMap, fieldOptions):
        """ Cleans up a dataframe that contains domain values

        Args:
            data = dataframe of domain values
            fieldMap: dict that maps columns of the dataframe csv to our db columns
            fieldOptions: dict with keys of attribute names, value contains a dict with options for that attribute.
                Current options are "pad_to_length" which if present will pad the field with leading zeros up to
                specified length, and "skip_duplicate" which ignores subsequent lines that repeat values.
        """
        # clean the dataframe column names
        data.rename(columns=lambda x: cls.cleanColNamesFunction(x), inplace=True)
        # make sure all values in fieldMap parameter are in the dataframe/csv file
        for field in fieldMap:
            if field not in list(data.columns):
                raise ValueError("{} is required for loading table{}".format(field, model))
        # toss out any columns from the csv that aren't in the fieldMap parameter
        data = data[list(fieldMap.keys())]
        # rename columns as specified in fieldMap
        data = data.rename(columns=fieldMap)

        # apply column options as specified in fieldOptions param
        for col, options in fieldOptions.items():
            if "pad_to_length" in options:
                # pad to specified length
                data['{}'.format(col)] = data['{}'.format(col)].apply(
                    lambda x: Validator.padToLength(x, padLength=options['pad_to_length']))
            if "skip_duplicates" in options and options['skip_duplicates']:
                # drop duplicates of specified fields
                # (keeps the row where the value first appears)
                data.drop_duplicates(subset=col, inplace=True)

        # add created_at and updated_at columns
        data = data.assign(
            created_at=cls.currentTimeFunction, updated_at=cls.currentTimeFunction)

        return data
