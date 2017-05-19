import pandas as pd

from datetime import datetime
from pandas import isnull


def clean_col_names(field):
    """Define some data-munging functions that can be applied to pandas
    dataframes as necessary"""
    return str(field).lower().strip().replace(" ", "_").replace(",", "_")


def pad_function(field, pad_to, keep_null):
    """Pads field to specified length."""
    if isnull(field) or not str(field).strip():
        if keep_null:
            return None
        else:
            field = ''
    return str(field).strip().zfill(pad_to)


def insert_dataframe(df, table, engine):
    """Inserts a dataframe to the specified database table."""
    df.to_sql(
        table,
        engine,
        index=False,
        if_exists='append'
    )
    return len(df.index)


def clean_data(data, model, field_map, field_options):
    """ Cleans up a dataframe that contains domain values.

    Parameters:
    ----------
        data : dataframe of domain values
        field_map: dict that maps columns of the dataframe csv to our db columns
        field_options: dict with keys of attribute names, value contains a dict with options for that attribute.
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
    data.rename(columns=clean_col_names, inplace=True)
    # make sure all values in fieldMap parameter are in the dataframe/csv file
    for field in field_map:
        if field not in list(data.columns):
            raise ValueError("{} is required for loading table{}".format(field, model))
    # toss out any columns from the csv that aren't in the fieldMap parameter
    data = data[list(field_map.keys())]
    # rename columns as specified in fieldMap
    data = data.rename(columns=field_map)

    # apply column options as specified in fieldOptions param
    for col, options in field_options.items():
        if 'pad_to_length' in options:
            # pad to specified length
            data[col] = data[col].apply(pad_function, args=(options['pad_to_length'], options.get('keep_null')))
        if options.get('strip_commas'):
            # remove commas for specified column
            # get rid of commas in dollar amounts
            data[col] = data[col].str.replace(",", "")

    # add created_at and updated_at columns
    now = datetime.utcnow()
    data = data.assign(created_at=now, updated_at=now)

    return data


def format_date(value):
    """ Format date from 'MMM dd, yyyy' to 'yyyymmdd' """

    formatted_value = pd.to_datetime(value, format="%b, %d %Y")
    formatted_value = formatted_value.apply(lambda x: x.strftime('%Y%m%d') if not pd.isnull(x) else '')
    return formatted_value
