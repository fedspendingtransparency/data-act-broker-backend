import numpy as np
import pandas as pd
import csv

from decimal import Decimal, DecimalException
from datetime import datetime
from pandas import isnull

from dataactcore.models.lookups import FIELD_TYPE_DICT_ID, FIELD_TYPE_DICT, FILE_TYPE_DICT_ID
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactvalidator.validation_handlers.validationError import ValidationError

BOOLEAN_VALUES = ['TRUE', 'FALSE', 'YES', 'NO', '1', '0']


def is_valid_type(data, data_type):
    """ Determine whether data is of the correct type

        Args:
            data: Data to be checked
            data_type: Type to check against

        Returns:
            True if data is of specified type, False otherwise
    """
    if data_type is None:
        # If no type specified, don't need to check anything
        return True
    if data is None or data.strip() == '':
        # All types can be None, and an empty string matches all types
        return True
    if data_type == 'STRING':
        return len(data) > 0
    if data_type == 'BOOLEAN':
        if data.upper() in BOOLEAN_VALUES:
            return True
        return False
    if data_type == 'INT':
        try:
            int(data)
            return True
        except ValueError:
            return False
    if data_type == 'DECIMAL':
        try:
            Decimal(data)
            return True
        except DecimalException:
            return False
    if data_type == 'LONG':
        try:
            int(data)
            return True
        except ValueError:
            return False
    raise ValueError(''.join(['Data Type Error, Type: ', data_type, ', Value: ', data]))


def clean_col(value, clean_quotes=True):
    """ Takes a value and returns None if it's empty, removes extra whitespace and surrounding quotes.

        Args:
            value: the value to clean
            clean_quotes: whether to clean extra quotes or not

        Returns:
            None if the value is empty or just whitespace, a stripped version of the value without surrounding quotes
            otherwise
    """
    # Need to check that it's not a list because truth value of a list will be an error
    if not isinstance(value, list) and (isnull(value) or not str(value).strip()):
        return None

    value = str(value).strip()
    if clean_quotes:
        # Trim and remove extra quotes around the outside. If removing quotes and stripping leaves nothing, return None
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1].strip()
            if not value:
                return None

    return value


def clean_frame_vectorized(frame: pd.DataFrame, convert_to_str=False, clean_quotes=True):
    """ In-place strip of surrounding whitespace, make None if empty, and remove surrounding quotes.

        Args:
            frame: pd.DataFrame to clean
            convert_to_str: whether to apply .astype(str) on all Series in the DataFrame
                - If the DataFrame was created or read in from CSV with dtype=str already, this may be unnecessary
                  and will save on performance if avoided
                - If left as False and there are mixed types, such as int or float, they will be set to None rather
                  than converted to their str form
                - If set as True, any non-null, non-string types provided (like int or float) will be converted to
                  their string format in the final result.
            clean_quotes: whether to clean extra quotes or not

        Returns:
            The cleaned DataFrame
    """
    if convert_to_str:
        frame = frame.apply(lambda series: series[series.notnull()].astype(str))
    # Do initial strip of surrounding whitespace
    frame = frame.apply(lambda series: series.str.strip())
    if clean_quotes:
        # NOTE: Need to find symmetry in the surrounding quotes in order to remove them.
        # Unbalanced quotes will not be removed
        for col, s in frame.items():
            s.update(s[(s.str.startswith('"')) & (s.str.endswith('"'))].str.strip('"').str.strip())
    # Null-out all empty strings remaining
    # NOTE: Must use python None here rather than numpy.NaN, because of downstream python code that is not NaN-aware
    frame = frame.mask(frame == "", other=None)
    # Favor None over np.NaN for downstream Python code that is not NaN-aware.
    # Must use dict here since method signature does not allow None for param value
    frame = frame.replace({np.NaN: None})
    return frame


def clean_numbers(value):
    """ Removes commas from strings representing numbers

        Args:
            value: the value to remove commas from

        Returns:
            The original value with commas removed if there were any
    """
    if value is not None:
        temp_value = str(value).replace(',', '')
        if FieldCleaner.is_numeric(temp_value):
            return temp_value
    return value


def clean_numbers_vectorized(series: pd.Series, convert_to_str=False):
    """ In-place removal of commas from strings representing numbers, or leaves them as-is if it cannot make a number

        Caveats (by Example):
                        A        B           C        D
            0  10,003,234  bad,and  2242424242  -10,000
            1           0        8     9.424.2      -10
            2     9.24242   ,209,4         ,01    ,-0,0
            3        1,45     0055        None      NaN
        Yields this evaluation of whether it can be cleaned:
                        A        B           C     D
            0        True    False        True  True
            1        True     True       False  True
            2        True     True        True  True
            3        True     True       False  True
        And these results when commas are replaced from those that can be cleaned
                        A        B           C       D
            0    10003234  bad,and  2242424242  -10000
            1           0        8     9.424.2     -10
            2     9.24242     2094          01     -00
            3         145     0055        None     NaN

        Args:
            series: the series that will be cleaned (updated in-place)
            convert_to_str: whether to apply .astype(str) on all Series in the DataFrame
                - If the DataFrame was created or read in from CSV with dtype=str already, this may be unnecessary
                  and will save on performance if avoided
                - If left as False and there are mixed types, such as int or float, they will be set to None rather
                  than converted to their str form
                - If set as True, any non-null, non-string types provided (like int or float) will be converted to
                  their string format in the final result.

        Returns:
            None (in-place update of given Series)
    """
    if convert_to_str:
        s = series[series.notnull()].astype(str)
    else:
        s = series
    # Get subset of values in series that had ',' replaced
    replacements = s[s.str.contains(",", na=False)].str.replace(",", "")
    # Check if their result is numeric
    cleanable = replacements.str.replace(".", "", 1).str.replace("-", "", 1).str.isdigit()
    # For those that are numeric, update the original series with their non-comma value
    series.update(replacements[cleanable])


def concat_flex(row):
    """ Concatenates the headers and contents of all the flex cells in one row of a submission and joins the list
        on commas

        Args:
            row: the dataframe row containing the submission row flex fields

        Returns:
            A concatenated list of "header: cell" pairs for the flex fields, joined by commas
    """
    return ', '.join([name + ': ' + (row[name] or '') for name in sorted(row.keys()) if name != 'row_number'])


def derive_unique_id(row, is_fabs):
    """ Derives the unique ID of a row and puts it in the proper format for the error/warning report.

        Args:
            row: the dataframe row to derive the unique ID for
            is_fabs: a boolean indicating if the submission is a FABS submission or not

        Returns:
            A properly formatted unique ID for the row depending on if it's a FABS or DABS submission
    """
    if not is_fabs:
        unique_id = 'TAS: {}'.format(row['display_tas'])
    else:
        unique_id = 'AssistanceTransactionUniqueKey: {}'.format(row['afa_generated_unique'])
    return unique_id


def derive_unique_id_vectorized(frame: pd.DataFrame, is_fabs):
    """ Derives the unique ID of a row and puts it in the proper format for the error/warning report.

        Args:
            frame: the pd.DataFrame whose 'unique_id' column's values will be updated
            is_fabs: a boolean indicating if the submission is a FABS submission or not

        Returns:
            A Series with properly formatted unique ID all rows depending on if it's a FABS or DABS submission
    """
    if not is_fabs:
        return 'TAS: ' + frame['display_tas'][frame['display_tas'].notnull()].astype(str)
    else:
        return 'AssistanceTransactionUniqueKey: ' + \
               frame['afa_generated_unique'][frame['afa_generated_unique'].notnull()].astype(str)


def derive_fabs_awarding_sub_tier(row, office_list):
    """ Derives the awarding sub tier agency code if it wasn't provided and can be derived from the office code.

        Args:
            row: the dataframe row to derive the awarding sub tier agency code for
            office_list: A dictionary of sub tier codes keyed by their office codes

        Returns:
            the results of trying to get the awarding sub tier agency code using the office code if there is no
            sub tier code provided, otherwise just returns the provided code
    """
    if not row['awarding_sub_tier_agency_c']:
        return office_list.get(row['awarding_office_code'])
    return row['awarding_sub_tier_agency_c']


def derive_fabs_afa_generated_unique(row):
    """ Derives the afa_generated_unique for a row.

        Args:
            row: the dataframe row to derive the unique key for

        Returns:
            The afa_generated_unique for the row
    """
    return (row['awarding_sub_tier_agency_c'] or '-none-') + '_' + \
           (row['fain'] or '-none-') + '_' + \
           (row['uri'] or '-none-') + '_' + \
           (row['assistance_listing_number'] or '-none-') + '_' + \
           (row['award_modification_amendme'] or '-none-')


def derive_fabs_unique_award_key(row):
    """ Derives the unique award key for a row.

        Args:
            row: the dataframe row to derive the unique award key for

        Returns:
            A unique award key for the row, generated based on record type and uppercased
    """
    if str(row['record_type']) == '1':
        unique_award_key_list = ['ASST_AGG', row['uri'] or '-none-']
    else:
        unique_award_key_list = ['ASST_NON', row['fain'] or '-none-']

    unique_award_key_list.append(row['awarding_sub_tier_agency_c'] or '-none-')

    return '_'.join(unique_award_key_list).upper()


def apply_label(row, labels, is_fabs):
    """ Get special rule labels for required or type checks for FABS submissions.

        Args:
            row: the dataframe row to get the label for
            labels: the list of labels that could be applied in this rule
            is_fabs: a boolean indicating if the submission is a FABS submission or not

        Returns:
            The label if it's a FABS submission and the header matches one of the ones there are labels for, empty
            string otherwise
    """
    if is_fabs and labels and row['Field Name'] in labels:
        return labels[row['Field Name']]
    return ''


def gather_flex_fields(row, flex_data):
    """ Getting the flex data, formatted for the error and warning report, for a row.

        Args:
            row: the dataframe row to get the flex data for
            flex_data: the dataframe containing flex fields for the file

        Returns:
            The concatenated flex data for the row if there is any, an empty string otherwise.
    """
    if flex_data is not None:
        return flex_data.loc[flex_data['row_number'] == row['Row Number'], 'concatted'].values[0]
    return ''


def valid_type(row, csv_schema):
    """ Checks if the value provided is of a valid type.

        Args:
            row: the dataframe row containing information about a cell, including the header and contents
            csv_schema: the schema containing the details about the columns for this file

        Returns:
            True or False depending on if the content of the cell is valid for the expected type
    """
    current_field = csv_schema[row['Field Name']]
    return is_valid_type(row['Value Provided'], FIELD_TYPE_DICT_ID[current_field.field_types_id])


def valid_type_bool_vector(series: pd.Series, csv_schema, type_field=None, match_invalid=False):
    """ Check datatype validity of each cell in a Series in a vectorized execution

        True/False will be determined by whether the value of the cell matches the datatype for the given
        series, according to the schema, and on the value of the `match_valid` toggle (see below).


        The returned bool vector can be used for Boolean Indexing of a DataFrame or Series, to pick on the
        valid/invalid values.

        Args:
            series: the pd.Series to act on, often provided as a column slice of a DataFrame.
            csv_schema: the schema containing the details about the columns for this file
            type_field: the column name this series represents. Used to lookup type information from the schema,
                        and should be provided if the `series.name` is not populated on the given `series`.
            match_invalid:
                - When left as `match_invalid=False`: True if the value matches its type; False otherwise.
                - When `match_invalid=True`: False if the value matches its type, True if it has an invalid type

        Returns:
            A boolean vector as a Series with True or False as values in the vector
    """
    field = type_field or series.name
    if not field:
        raise ValueError("Cannot lookup field type info from schema because the given Series has no name and "
                         "type_field was not provided. One or the other must be set.")
    required_type = FIELD_TYPE_DICT_ID[csv_schema[field].field_types_id]
    if match_invalid:
        return series.map(lambda x: not is_valid_type(x, required_type))
    return series.map(lambda x: is_valid_type(x, required_type))


def invalid_type_vector(series: pd.Series, csv_schema, type_field=None):
    """ Provide a Series in a vectorized operation where values with valid datatypes are nulled-out (set to NaN),
        only leaving invalid values. Useful for building error reports.

        Args:
            series: the pd.Series to act on, often provided as a column slice of a DataFrame.
            csv_schema: the schema containing the details about the columns for this file
            type_field: the column name this series represents. Used to lookup type information from the schema,
                        and should be provided if the `series.name` is not populated on the given `series`.

        Returns:
            A new Series with only invalid datatypes remaining from the one provided.
    """
    return series.where(valid_type_bool_vector(series, csv_schema, type_field, match_invalid=True))


def expected_type(row, csv_schema):
    """ Formats and returns an error message explaining what the expected type of a field is.

        Args:
            row: the dataframe row containing information about a cell, including the header and contents
            csv_schema: the schema containing the details about the columns for this file

        Returns:
            A formatted message explaining what type the field should be
    """
    current_field = csv_schema[row['Field Name']]
    return 'This field must be a {}'.format(FIELD_TYPE_DICT_ID[current_field.field_types_id].lower())


def valid_length(row, csv_schema):
    """ Checks if the value provided is longer than the maximum allowed length for a particular field.

        Args:
            row: the dataframe row containing information about a cell, including the header and contents
            csv_schema: the schema containing the details about the columns for this file

        Returns:
            True if the value is an acceptable length or doesn't have a maximum length, False otherwise
    """
    current_field = csv_schema[row['Field Name']]
    if current_field.length:
        return len(row['Value Provided']) <= current_field.length
    return True


def valid_length_bool_vector(series: pd.Series, csv_schema, type_field=None, match_invalid=False):
    """ Check length validity of each cell in a Series in a vectorized execution

        True/False will be determined by whether the value of the cell does not exceed the max length fo the field
        for the given series, according to the schema, and on the value of the `match_valid` toggle (see below).

        The returned bool vector can be used for Boolean Indexing of a DataFrame or Series, to pick on the
        valid/invalid values.

        Args:
            series: the pd.Series to act on, often provided as a column slice of a DataFrame.
                - NOTE: The values must be strings (or null/NaN) to evaluate their length
            csv_schema: the schema containing the details about the columns for this file
            type_field: the column name this series represents. Used to lookup type information from the schema,
                        and should be provided if the `series.name` is not populated on the given `series`.
            match_invalid:
                - When left as `match_invalid=False`: True if the value is within max length; False otherwise.
                - When `match_invalid=True`: False if the value is within max length, True if it exceeds max length

        Returns:
            A boolean vector as a Series with True or False as values in the vector
    """
    field = type_field or series.name
    if not field:
        raise ValueError("Cannot lookup field type info from schema because the given Series has no name and "
                         "type_field was not provided. One or the other must be set.")
    if match_invalid:
        return series.str.len() > csv_schema[field].length
    return series.str.len() <= csv_schema[field].length


def invalid_length_vector(series: pd.Series, csv_schema, type_field=None):
    """ Provide a Series in a vectorized operation where values with that meet there string length requirements are
        nulled-out (set to NaN), only leaving invalid values exceeding max length. Useful for building error reports.

        Args:
            series: the pd.Series to act on, often provided as a column slice of a DataFrame.
                - NOTE: The values must be strings (or null/NaN) to evaluate their length
            csv_schema: the schema containing the details about the columns for this file
            type_field: the column name this series represents. Used to lookup type information from the schema,
                        and should be provided if the `series.name` is not populated on the given `series`.

        Returns:
            A new Series with only invalid values (exceeding max length) remaining from the one provided.
    """
    return series.where(valid_length_bool_vector(series, csv_schema, type_field, match_invalid=True))


def expected_length(row, csv_schema):
    """ Formats and returns an error message explaining what the maximum length of a field is.

            Args:
                row: the dataframe row containing information about a cell, including the header and contents
                csv_schema: the schema containing the details about the columns for this file

            Returns:
                A formatted message explaining what the maximum length of the field is
    """
    current_field = csv_schema[row['Field Name']]
    return 'Max length: {}'.format(current_field.length)


def valid_format(row):
    """ Checks if the value provided is formatted correctly (dates must be YYYYMMDD format).

        Args:
            row: the dataframe row containing information about a cell, including the header and contents

        Returns:
            True if the value is formatted correctly, False otherwise
    """
    current_value = row['Value Provided']
    # Make sure the length is right
    if len(current_value) != 8:
        return False
    # Try to convert it using this specific format. If it doesn't work, it's not formatted right.
    try:
        datetime.strptime(current_value, '%Y%m%d')
    except ValueError:
        return False
    return True


def update_field_name(row, short_cols):
    """ Update all field names provided to match the lowercased GSDM headers rather than the database names

        Args:
            row: the dataframe row containing information about a cell, including the header and contents
            short_cols: A dictionary of lowercased GSDM headers keyed by database column names

        Returns:
            The GSDM version of the header if it can be derived from the column list, otherwise the header provided
    """
    if row['Field Name'] in short_cols:
        return short_cols[row['Field Name']]
    return row['Field Name']


def add_field_name_to_value(row):
    """ Combine the field name and value provided into one string.

        Args:
            row: the dataframe row containing information about a cell, including the header and contents

        Returns:
            The field name and value provided combined into one string
    """
    return row['Field Name'] + ': ' + row['Value Provided']


def check_required(data, required, required_labels, report_headers, short_cols, flex_data, is_fabs):
    """ Check if all fields that are required to have content in the file have content.

        Args:
            data: the dataframe containing the data for the submission
            required: A list of headers that represent the required fields in the file
            required_labels: A mapping of labels that will get added to required field errors in FABS submissions
            report_headers: The list of error/warning report headers in order
            short_cols: A mapping of the database column names to the lowercased GSDM headers
            flex_data: the dataframe containing flex data for this file
            is_fabs: A boolean indicating if this is a FABS submission or not

        Returns:
            A dataframe containing error text that can be turned into an error report for required fields
    """
    # Get just the required columns along with the row number and unique ID
    req_data = data[required + ['row_number', 'unique_id']]
    # Flip the data so each header + cell combination is its own row, keeping the relevant row numbers and unique IDs
    errors = pd.melt(req_data, id_vars=['row_number', 'unique_id'], value_vars=required, var_name='Field Name',
                     value_name='Value Provided')
    # Throw out all rows that have data
    errors = errors[errors['Value Provided'].isnull()]
    errors.rename(columns={'row_number': 'Row Number', 'unique_id': 'Unique ID'}, inplace=True)
    errors = errors.reset_index()
    errors['Value Provided'] = ''
    errors['Rule Message'] = ValidationError.requiredErrorMsg
    errors['Expected Value'] = '(not blank)'
    errors['Difference'] = ''
    if not errors.empty:
        errors['Rule Label'] = errors.apply(lambda x: apply_label(x, required_labels, is_fabs), axis=1)
        errors['Flex Field'] = errors.apply(lambda x: gather_flex_fields(x, flex_data), axis=1)
        errors['Field Name'] = errors.apply(lambda x: update_field_name(x, short_cols), axis=1)
    else:
        errors['Rule Label'] = ''
        errors['Flex Field'] = ''
    # sorting the headers after all the moving around
    errors = errors[report_headers]
    errors['error_type'] = ValidationError.requiredError
    return errors


def check_type(data, type_fields, type_labels, report_headers, csv_schema, short_cols, flex_data, is_fabs):
    """ Check if all fields that are a type other than string match that type.

        Args:
            data: the dataframe containing the data for the submission
            type_fields: A list of headers that represent the non-string fields in the file
            type_labels: A mapping of labels that will get added to non-string field errors in FABS submissions
            report_headers: The list of error/warning report headers in order
            csv_schema: the schema containing the details about the columns for this file
            short_cols: A mapping of the database column names to the lowercased GSDM headers
            flex_data: the dataframe containing flex data for this file
            is_fabs: A boolean indicating if this is a FABS submission or not

        Returns:
            A dataframe containing error text that can be turned into an error report for non-string fields
    """
    # Get just the non-string columns along with the row number and unique ID
    # We will be modifying, by nulling the valid types, so get a copy so as to not modify the original data
    invalid_datatype = data[type_fields + ['row_number', 'unique_id']].copy()

    for type_field in type_fields:
        # For each col-Series, null-out (set to NaN) any cells that meet datatype requirements
        invalid_datatype[type_field] = invalid_type_vector(invalid_datatype[type_field], csv_schema)
    # Flip the data so each header + cell combination is its own row, keeping the relevant row numbers and unique IDs
    errors = pd.melt(invalid_datatype, id_vars=['row_number', 'unique_id'], value_vars=type_fields,
                     var_name='Field Name', value_name='Value Provided')

    # Throw out rows for all cell values that were compliant or originally null
    errors = errors[~(errors['Value Provided'].isnull())]
    errors.rename(columns={'row_number': 'Row Number', 'unique_id': 'Unique ID'}, inplace=True)
    errors = errors.reset_index()
    errors['Rule Message'] = ValidationError.typeErrorMsg
    errors['Difference'] = ''
    if not errors.empty:
        errors['Expected Value'] = errors.apply(lambda x: expected_type(x, csv_schema), axis=1)
        errors['Rule Label'] = errors.apply(lambda x: apply_label(x, type_labels, is_fabs), axis=1)
        errors['Flex Field'] = errors.apply(lambda x: gather_flex_fields(x, flex_data), axis=1)
        errors['Field Name'] = errors.apply(lambda x: update_field_name(x, short_cols), axis=1)
        errors['Value Provided'] = errors.apply(lambda x: add_field_name_to_value(x), axis=1)
    else:
        errors['Expected Value'] = ''
        errors['Rule Label'] = ''
        errors['Flex Field'] = ''
    # sorting the headers after all the moving around
    errors = errors[report_headers]
    errors['error_type'] = ValidationError.typeError
    return errors


def check_length(data, length_fields, report_headers, csv_schema, short_cols, flex_data, type_error_rows):
    """ Check if all fields that have a maximum length are at or under that length.

        Args:
            data: the dataframe containing the data for the submission
            length_fields: A list of headers that represent the fields in the file with maximum lengths
            report_headers: The list of error/warning report headers in order
            csv_schema: the schema containing the details about the columns for this file
            short_cols: A mapping of the database column names to the lowercased GSDM headers
            flex_data: the dataframe containing flex data for this file
            type_error_rows: A list of row numbers indicating what rows have type errors

        Returns:
            A dataframe containing error text that can be turned into an error report for fields that are too long
    """
    # Drop all rows that have a type error
    exceeds_length = data[~(data['row_number'].isin(type_error_rows))]

    # Get just the columns with a maximum length along with the row number and unique ID
    # We will be modifying, by nulling the valid types, so get a copy so as to not modify the original data
    exceeds_length = exceeds_length[length_fields + ['row_number', 'unique_id']].copy()

    for length_field in length_fields:
        # For each col-Series, null-out (set to NaN) any cells that meet max length requirements
        exceeds_length[length_field] = invalid_length_vector(exceeds_length[length_field], csv_schema)

    # Flip the data so each header + cell combination is its own row, keeping the dfrelevant row numbers and unique IDs
    errors = pd.melt(exceeds_length, id_vars=['row_number', 'unique_id'], value_vars=length_fields,
                     var_name='Field Name', value_name='Value Provided')
    # Throw out rows for all cell values that were compliant or originally null
    errors = errors[~(errors['Value Provided'].isnull())]
    errors.rename(columns={'row_number': 'Row Number', 'unique_id': 'Unique ID'}, inplace=True)
    errors = errors.reset_index()
    errors['Rule Message'] = ValidationError.lengthErrorMsg
    errors['Difference'] = ''
    errors['Rule Label'] = ''
    if not errors.empty:
        errors['Expected Value'] = errors.apply(lambda x: expected_length(x, csv_schema), axis=1)
        errors['Flex Field'] = errors.apply(lambda x: gather_flex_fields(x, flex_data), axis=1)
        errors['Field Name'] = errors.apply(lambda x: update_field_name(x, short_cols), axis=1)
        errors['Value Provided'] = errors.apply(lambda x: add_field_name_to_value(x), axis=1)
    else:
        errors['Expected Value'] = ''
        errors['Flex Field'] = ''
    # sorting the headers after all the moving around
    errors = errors[report_headers]
    errors['error_type'] = ValidationError.lengthError
    return errors


def check_field_format(data, format_fields, report_headers, short_cols, flex_data):
    """ Check if all the fields are in the appropriate format.

        Args:
            data: the dataframe containing the data for the submission
            format_fields: A list of headers that represent the format check fields in the file
            report_headers: The list of error/warning report headers in order
            short_cols: A mapping of the database column names to the lowercased GSDM headers
            flex_data: the dataframe containing flex data for this file

        Returns:
            A dataframe containing error text that can be turned into an error report for non-string fields
    """
    # Get just the non-string columns along with the row number and unique ID
    type_data = data[format_fields + ['row_number', 'unique_id']]
    # Flip the data so each header + cell combination is its own row, keeping the relevant row numbers and unique IDs
    errors = pd.melt(type_data, id_vars=['row_number', 'unique_id'], value_vars=format_fields,
                     var_name='Field Name', value_name='Value Provided')
    # Throw out all rows that don't have data, they don't have a format check
    errors = errors[~errors['Value Provided'].isnull()]
    # If there is data that needs checking, keep only the data that doesn't have the right type
    if not errors.empty:
        errors['matches_format'] = errors.apply(lambda x: valid_format(x), axis=1)
        errors = errors[~errors['matches_format']]
        errors.drop(['matches_format'], axis=1, inplace=True)
    errors.rename(columns={'row_number': 'Row Number', 'unique_id': 'Unique ID'}, inplace=True)
    errors = errors.reset_index()
    errors['Rule Message'] = ValidationError.fieldFormatErrorMsg
    errors['Difference'] = ''
    errors['Rule Label'] = 'DABSDATETIME'
    errors['Expected Value'] = 'A date in the YYYYMMDD format.'
    if not errors.empty:
        errors['Flex Field'] = errors.apply(lambda x: gather_flex_fields(x, flex_data), axis=1)
        errors['Field Name'] = errors.apply(lambda x: update_field_name(x, short_cols), axis=1)
        errors['Value Provided'] = errors.apply(lambda x: add_field_name_to_value(x), axis=1)
    else:
        errors['Flex Field'] = ''
    # sorting the headers after all the moving around
    errors = errors[report_headers]
    errors['error_type'] = ValidationError.fieldFormatError
    return errors


def parse_fields(sess, fields):
    """ Parse through all the fields in the file type and sort them into the relevant rule lists.

        Args:
            sess: the database connection
            fields: The fields in the file header

        Returns:
            A list of the names of the headers that this type of file should have and a dictionary containing lists
            of headers that are: required, must be some kind of number, must be a boolean, have a maximum length,
            and need to be padded if they're too short
    """
    parsed_fields = {
        'required': [],
        'number': [],
        'boolean': [],
        'format': [],
        'length': [],
        'padded': []
    }
    expected_headers = []
    number_field_types = [FIELD_TYPE_DICT['INT'], FIELD_TYPE_DICT['DECIMAL'], FIELD_TYPE_DICT['LONG']]
    for field in fields:
        # Create a list of just the header names
        expected_headers.append(field.name_short)

        # Add fields to the lists in the dictionary that they are relevant to (type, length, etc)
        if field.field_types_id in number_field_types:
            parsed_fields['number'].append(field.name_short)
        elif field.field_types_id == FIELD_TYPE_DICT['BOOLEAN']:
            parsed_fields['boolean'].append(field.name_short)
        elif field.field_types_id == FIELD_TYPE_DICT['DATE']:
            parsed_fields['format'].append(field.name_short)
        if field.required:
            parsed_fields['required'].append(field.name_short)
        if field.length:
            parsed_fields['length'].append(field.name_short)
        if field.padded_flag:
            parsed_fields['padded'].append(field.name_short)
        sess.expunge(field)
    return expected_headers, parsed_fields


def process_formatting_errors(short_rows, long_rows, report_headers):
    """ Creates a dataframe containing all the formatting errors in the file.

        Args:
            short_rows: A list of row numbers where there were not enough cells in the row
            long_rows: A list of row numbers where there were too many cells in the row
            report_headers: The list of error/warning report headers in order

        Returns:
            A dataframe containing error text that can be turned into an error report for poorly formatted CSV rows
    """
    format_error_list = []
    # Create a list of dictionaries that contain information about poorly formatted rows
    for format_row in sorted(short_rows + long_rows):
        format_error = {
            'Unique ID': '',
            'Field Name': 'Formatting Error',
            'Rule Message': ValidationError.readErrorMsg,
            'Value Provided': '',
            'Expected Value': '',
            'Difference': '',
            'Flex Field': '',
            'Row Number': str(format_row),
            'Rule Label': '',
            'error_type': ValidationError.readError
        }
        format_error_list.append(format_error)
    # Turn the list of dictionaries into a dataframe
    return pd.DataFrame(format_error_list, columns=list(report_headers + ['error_type']))


def simple_file_scan(reader, bucket_name, region_name, file_name):
    """ Does an initial scan of the file, figuring out the file row count and which rows are too long/short

        Args:
            reader: the csv reader
            bucket_name: the bucket to pull from
            region_name: the region to pull from
            file_name: name of the file to pull

        Returns:
            file_row_count: the number of lines in the file
            short_rows: a list of row numbers that have too few fields
            long_rows: a list of rows that have too many fields
    """
    # Count file rows: throws a File Level Error for non-UTF8 characters
    # Also getting short and long rows for formatting errors and pandas processing
    temp_file = open(reader.get_filename(region_name, bucket_name, file_name), encoding='utf-8')
    file_row_count = 0
    header_length = 0
    short_pop_rows = []
    long_pop_rows = []
    short_null_rows = []
    long_null_rows = []
    # Getting the delimiter
    header_line = temp_file.readline()
    delimiter = '|' if header_line.count('|') > header_line.count(',') else ','
    temp_file.seek(0)
    for line in csv.reader(temp_file, delimiter=delimiter):
        if line:
            file_row_count += 1
            line_length = len(line)
            # Setting the expected length for the file
            if header_length == 0:
                header_length = line_length
            # All lines that are shorter than they should be
            elif line_length < header_length:
                # do not add short row if there's no data in the row
                if len(''.join(line)) > 0:
                    short_pop_rows.append(file_row_count)
                else:
                    short_null_rows.append(file_row_count)
            # All lines that are longer than they should be
            elif line_length > header_length:
                # do not add short row if there's no data in the row
                if len(''.join(line)) > 0:
                    long_pop_rows.append(file_row_count)
                else:
                    long_null_rows.append(file_row_count)
    try:
        temp_file.close()
    except AttributeError:
        # File does not exist, and so does not need to be closed
        pass
    return file_row_count, short_pop_rows, long_pop_rows, short_null_rows, long_null_rows


def update_val_progress(sess, job, validation_progress, tas_progress, sql_progress, final_progress):
    """ Updates the progress value of the job based on the type of validation it is.

        Args:
            sess: the database session
            job: the job being updated
            validation_progress: how much of the file has finished its initial validation steps
            tas_progress: whether the file has completed tas linking or not (realistically always 0 or 100)
            sql_progress: how far through the SQL validations the job has progressed
            final_progress: how far through the post-SQL cleanup the job has progressed
    """

    val_mult = .2 if FILE_TYPE_DICT_ID[job.file_type_id] != 'fabs' else .4
    tas_mult = .25 if FILE_TYPE_DICT_ID[job.file_type_id] != 'fabs' else 0
    sql_mult = .5
    final_mult = .05 if FILE_TYPE_DICT_ID[job.file_type_id] != 'fabs' else .1

    current_progress = validation_progress * val_mult + tas_progress * tas_mult + sql_progress * sql_mult + \
        final_progress * final_mult
    job.progress = current_progress
    sess.commit()


def update_cross_val_progress(sess, job, pairs_finished, set_length, completed_in_set):
    """ Updates the progress value of the cross-file job based on how many rules have been completed

        Args:
            sess: the database session
            job: the job being updated
            pairs_finished: how many of the 4 pairs have been completed before starting this update
            set_length: how many rules are in this set of rules
            completed_in_set: how many of the rules in this set have been completed
    """

    # Multiplying by 25 because there are 4 total sets of cross-file so we want each to count as 25%
    job.progress = pairs_finished * 25 + (completed_in_set / set_length) * 25
    sess.commit()
