import pandas as pd
import csv
import re

from decimal import Decimal, DecimalException
from datetime import datetime
from pandas import isnull

from dataactcore.models.lookups import FIELD_TYPE_DICT_ID, FIELD_TYPE_DICT
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
    if data.strip() == '':
        # An empty string matches all types
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
    if isnull(value) or not str(value).strip():
        return None

    value = str(value).strip()
    if clean_quotes:
        # Trim and remove extra quotes around the outside. If removing quotes and stripping leaves nothing, return None
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1].strip()
            if not value:
                return None

    return value


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


def concat_flex(row):
    """ Concatenates the headers and contents of all the flex cells in one row of a submission and joins the list
        on commas

        Args:
            row: the dataframe row containing the submission row flex fields

        Returns:
            A concatenated list of "header: cell" pairs for the flex fields, joined by commas
    """
    return ', '.join([name + ': ' + (row[name] or '') for name in sorted(row.keys()) if name is not 'row_number'])


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
           (row['cfda_number'] or '-none-') + '_' + \
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
    # Try to convert it using this specific format. If it doesn't work, it's not formatted right.
    try:
        datetime.strptime(row['Value Provided'], '%Y%m%d')
    except ValueError:
        return False
    return True


def update_field_name(row, short_cols):
    """ Update all field names provided to match the lowercased DAIMS headers rather than the database names

        Args:
            row: the dataframe row containing information about a cell, including the header and contents
            short_cols: A dictionary of lowercased DAIMS headers keyed by database column names

        Returns:
            The DAIMS version of the header if it can be derived from the column list, otherwise the header provided
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
            short_cols: A mapping of the database column names to the lowercased DAIMS headers
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
    errors['Error Message'] = ValidationError.requiredErrorMsg
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
            short_cols: A mapping of the database column names to the lowercased DAIMS headers
            flex_data: the dataframe containing flex data for this file
            is_fabs: A boolean indicating if this is a FABS submission or not

        Returns:
            A dataframe containing error text that can be turned into an error report for non-string fields
    """
    # Get just the non-string columns along with the row number and unique ID
    type_data = data[type_fields + ['row_number', 'unique_id']]
    # Flip the data so each header + cell combination is its own row, keeping the relevant row numbers and unique IDs
    errors = pd.melt(type_data, id_vars=['row_number', 'unique_id'], value_vars=type_fields,
                     var_name='Field Name', value_name='Value Provided')
    # Throw out all rows that don't have data, they don't have a type
    errors = errors[~errors['Value Provided'].isnull()]
    # If there is data that needs checking, keep only the data that doesn't have the right type
    if not errors.empty:
        errors['matches_type'] = errors.apply(lambda x: valid_type(x, csv_schema), axis=1)
        errors = errors[~errors['matches_type']]
        errors.drop(['matches_type'], axis=1, inplace=True)
    errors.rename(columns={'row_number': 'Row Number', 'unique_id': 'Unique ID'}, inplace=True)
    errors = errors.reset_index()
    errors['Error Message'] = ValidationError.typeErrorMsg
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
            short_cols: A mapping of the database column names to the lowercased DAIMS headers
            flex_data: the dataframe containing flex data for this file
            type_error_rows: A list of row numbers indicating what rows have type errors

        Returns:
            A dataframe containing error text that can be turned into an error report for fields that are too long
    """
    # Get just the columns with a maximum length along with the row number and unique ID
    length_data = data[length_fields + ['row_number', 'unique_id']]
    # Flip the data so each header + cell combination is its own row, keeping the relevant row numbers and unique IDs
    errors = pd.melt(length_data, id_vars=['row_number', 'unique_id'], value_vars=length_fields,
                     var_name='Field Name', value_name='Value Provided')
    # Throw out all rows that don't have data or have a type error
    errors = errors[~errors['Value Provided'].isnull() & ~errors['row_number'].isin(type_error_rows)]
    # If there is data that needs checking, keep only the data that is too long
    if not errors.empty:
        errors['valid_length'] = errors.apply(lambda x: valid_length(x, csv_schema), axis=1)
        errors = errors[~errors['valid_length']]
        errors.drop(['valid_length'], axis=1, inplace=True)
    errors.rename(columns={'row_number': 'Row Number', 'unique_id': 'Unique ID'}, inplace=True)
    errors = errors.reset_index()
    errors['Error Message'] = ValidationError.lengthErrorMsg
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
    """ Check if all fields that are a type other than string match that type.

        Args:
            data: the dataframe containing the data for the submission
            format_fields: A list of headers that represent the format check fields in the file
            report_headers: The list of error/warning report headers in order
            short_cols: A mapping of the database column names to the lowercased DAIMS headers
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
    errors['Error Message'] = ValidationError.fieldFormatErrorMsg
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
            'Error Message': ValidationError.readErrorMsg,
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
    short_rows = []
    long_rows = []
    for line in csv.reader(temp_file):
        if line:
            file_row_count += 1
            line_length = len(line)
            # Setting the expected length for the file
            if header_length == 0:
                header_length = line_length
            # All lines that are shorter than they should be
            elif line_length < header_length:
                short_rows.append(file_row_count)
            # All lines that are longer than they should be
            elif line_length > header_length:
                long_rows.append(file_row_count)
    try:
        temp_file.close()
    except AttributeError:
        # File does not exist, and so does not need to be closed
        pass
    return file_row_count, short_rows, long_rows
