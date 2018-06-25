import logging
import pandas as pd
import numpy as np

from datetime import datetime
from pandas import isnull
from dataactcore.utils.failure_threshold_exception import FailureThresholdExceededException

logger = logging.getLogger(__name__)

FAILURE_THRESHOLD_PERCENTAGE = .01


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


def trim_item(item):
    if type(item) == np.str:
        return item.strip()
    return item


def clean_data(data, model, field_map, field_options, required_values=[], return_dropped_count=False):

    """ Cleans up a dataframe that contains domain values.

    Args:
        data : dataframe of domain values
        field_map: dict that maps columns of the dataframe csv to our db columns
        field_options: dict with keys of attribute names, value contains a dict with options for that attribute.
            Current options are:
             "pad_to_length" which if present will pad the field with leading zeros up to
            specified length
            "keep_null" when set to true, empty fields will not be padded
            "skip_duplicate" which ignores subsequent lines that repeat values
            "strip_commas" which removes commas
    Returns:
        Dataframe conforming to requirements, and additionally the number of rows dropped for missing value
        if return_dropped_count argument is True

    Raises:
        FailureThresholdExceededException: If too many rows have been discarded during processing, fail the routine.
           Also fail if the file is blank.

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

    # trim all columns
    data = data.applymap(lambda x: trim_item(x) if len(str(x).strip()) else None)

    if len(required_values) > 0:
        # if file is blank, immediately fail
        if data.empty or len(data.shape) < 2:
            raise FailureThresholdExceededException(0)
        # check the columns that must have a valid value, and if they have white space,
        # replace with NaN so that dropna finds them.
        for value in required_values:
            data[value].replace('', np.nan, inplace=True)
        # drop any rows that are missing required data
        cleaned = data.dropna(subset=required_values)
        dropped = data[np.invert(data.index.isin(cleaned.index))]
        # log every dropped row
        for index, row in dropped.iterrows():
            logger.info("Dropped row due to faulty data: fyq:{}--agency:{}--alloc:{}--account:{}".format(
                row["fiscal_year_quarter"],
                row['agency_id'], row['allocation_transfer_id'], row['account_number']) +
                "--pa_code:{}--pa_name:{}".format(row['program_activity_code'], row['program_activity_name']))

        if (len(dropped.index)/len(cleaned.index)) > FAILURE_THRESHOLD_PERCENTAGE:
            raise FailureThresholdExceededException(len(dropped.index))
        logger.info("{} total rows dropped due to faulty data".format(len(dropped.index)))
        data = cleaned

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
    if return_dropped_count:
        return len(dropped.index), data
    else:
        return data


def format_date(value):
    """ Format date from 'MMM dd, yyyy' to 'yyyymmdd' """

    formatted_value = pd.to_datetime(value, format="%b %d,%Y")
    formatted_value = formatted_value.apply(lambda x: x.strftime('%Y%m%d') if not pd.isnull(x) else '')
    return formatted_value
