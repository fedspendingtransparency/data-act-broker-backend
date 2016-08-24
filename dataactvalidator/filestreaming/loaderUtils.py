from datetime import datetime
from pandas import isnull


class LoaderUtils:

    # define some data-munging functions that can be applied to
    # pandas dataframes as necessary
    currentTimeFunction = lambda x: datetime.utcnow()
    cleanColNamesFunction = lambda field: str(field).lower().strip().replace(" ","_").replace(",","_")

    @classmethod
    def padFunction(self, field, padTo):
        """Pads field to specified length."""
        if field is None or isnull(field):
            field = ''
        return str(field).strip().zfill(padTo)

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
                Current options are:
                 "pad_to_length" which if present will pad the field with leading zeros up to
                specified length
                "skip_duplicate" which ignores subsequent lines that repeat values
                "strip_commas" which removes commas
        """
        # toss out blank rows
        data.dropna(inplace=True, how='all')

        # Fix up cells that have spaces instead of being empty.
        # Set the truly empty cells to None so they get inserted to db as NULL
        # TODO: very ugly function below...is there a better way?
        data.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)

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
                    cls.padFunction, args=(options['pad_to_length'],))
            if options.get('strip_commas'):
                # remove commas for specified column
                # get rid of commas in dollar amounts
                data[col] = data[col].str.replace(",", "")

        # add created_at and updated_at columns
        data = data.assign(
            created_at=cls.currentTimeFunction, updated_at=cls.currentTimeFunction)

        return data

    @classmethod
    def insertDataframe(cls, df, table, engine):
        """Inserts a dataframe to the specified database table."""
        df.to_sql(
            table,
            engine,
            index=False,
            if_exists='append'
        )
        print('{} records inserted to {}'.format(len(df.index), table))
