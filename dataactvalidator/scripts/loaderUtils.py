from datetime import datetime
from pandas import isnull


class LoaderUtils:

    # define some data-munging functions that can be applied to
    # pandas dataframes as necessary
    cleanColNamesFunction = lambda field: str(field).lower().strip().replace(" ","_").replace(",","_")

    @classmethod
    def padFunction(self, field, padTo, keepNull):
        """Pads field to specified length."""
        if isnull(field) or not str(field).strip():
            if keepNull:
                return None
            else:
                field = ''
        return str(field).strip().zfill(padTo)


    @classmethod
    def cleanData(cls, data, model, fieldMap, fieldOptions):
        """ Cleans up a dataframe that contains domain values.

        Parameters:
        ----------
            data : dataframe of domain values
            fieldMap: dict that maps columns of the dataframe csv to our db columns
            fieldOptions: dict with keys of attribute names, value contains a dict with options for that attribute.
                Current options are:
                 "pad_to_length" which if present will pad the field with leading zeros up to
                specified length
                "keep_null" when set to true, empty fields will not be padded
                "skip_duplicate" which ignores subsequent lines that repeat values
                "strip_commas" which removes commas
        """
        # incoming .csvs often have extraneous blank rows at the end,
        # so get rid of those
        data.dropna(inplace=True, how='all')

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
            if 'pad_to_length' in options:
                # pad to specified length
                data[col] = data[col].apply(
                    cls.padFunction, args=(
                        options['pad_to_length'],
                        options.get('keep_null')))
            if options.get('strip_commas'):
                # remove commas for specified column
                # get rid of commas in dollar amounts
                data[col] = data[col].str.replace(",", "")

        # add created_at and updated_at columns
        now = datetime.utcnow()
        data = data.assign(
            created_at=now, updated_at=now)

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
        return len(df.index)
