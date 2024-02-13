import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
import os

import pytest

from dataactbroker.helpers import validation_helper
from dataactvalidator.app import ValidationManager, ValidationError
from dataactvalidator.filestreaming.csvReader import CsvReader
from dataactcore.models.validationModels import FileColumn
from dataactcore.models.lookups import FIELD_TYPE_DICT, JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT

from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory

FILES_DIR = os.path.join('tests', 'integration', 'data')
READ_ERROR = os.path.join(FILES_DIR, 'appropReadError.csv')
BLANK_C = os.path.join(FILES_DIR, 'awardFinancialBlank.csv')


def test_is_valid_type():
    assert validation_helper.is_valid_type(None, 'STRING') is True
    assert validation_helper.is_valid_type(None, 'STRING') is True
    assert validation_helper.is_valid_type(None, 'INT') is True
    assert validation_helper.is_valid_type(None, 'DECIMAL') is True
    assert validation_helper.is_valid_type(None, 'BOOLEAN') is True
    assert validation_helper.is_valid_type(None, 'LONG') is True

    assert validation_helper.is_valid_type('1234Test', 'STRING') is True
    assert validation_helper.is_valid_type('1234Test', 'INT') is False
    assert validation_helper.is_valid_type('1234Test', 'DECIMAL') is False
    assert validation_helper.is_valid_type('1234Test', 'BOOLEAN') is False
    assert validation_helper.is_valid_type('1234Test', 'LONG') is False

    assert validation_helper.is_valid_type('', 'STRING') is True
    assert validation_helper.is_valid_type('', 'INT') is True
    assert validation_helper.is_valid_type('', 'DECIMAL') is True
    assert validation_helper.is_valid_type('', 'BOOLEAN') is True
    assert validation_helper.is_valid_type('', 'LONG') is True

    assert validation_helper.is_valid_type('01234', 'STRING') is True
    assert validation_helper.is_valid_type('01234', 'INT') is True
    assert validation_helper.is_valid_type('01234', 'DECIMAL') is True
    assert validation_helper.is_valid_type('01234', 'LONG') is True
    assert validation_helper.is_valid_type('01234', 'BOOLEAN') is False

    assert validation_helper.is_valid_type('1234.0', 'STRING') is True
    assert validation_helper.is_valid_type('1234.0', 'INT') is False
    assert validation_helper.is_valid_type('1234.00', 'DECIMAL') is True
    assert validation_helper.is_valid_type('1234.0', 'LONG') is False
    assert validation_helper.is_valid_type('1234.0', 'BOOLEAN') is False


def test_clean_col():
    # None cases
    assert validation_helper.clean_col('') is None
    assert validation_helper.clean_col('  ') is None
    assert validation_helper.clean_col('\n') is None
    assert validation_helper.clean_col('\"\"') is None
    assert validation_helper.clean_col(np.nan) is None
    assert validation_helper.clean_col(None) is None

    # clean cases
    assert validation_helper.clean_col('\nclean me! ') == "clean me!"
    assert validation_helper.clean_col(0) == '0'
    assert validation_helper.clean_col(1) == '1'
    assert validation_helper.clean_col(' \" double quotes\"') == 'double quotes'
    assert validation_helper.clean_col([]) == '[]'
    assert validation_helper.clean_col({}) == '{}'


def test_clean_frame_vectorized():
    df_under_test = pd.DataFrame([
        ['""', "", " lspace", '"lquote'],
        ["''", " ", "rspace ", 'rquote"'],
        ["'hello'", "  ", " surround space ", '"surround quote"'],
        ['"hello"', "\n\t", None, '" surround quote and space "'],
        ['"hello you"', "5", np.NaN, ' " surround quote and space "\t'],
    ], columns=list("ABCD"))

    df_under_test = validation_helper.clean_frame_vectorized(df_under_test)

    expected_df = pd.DataFrame([
        [None, None, "lspace", '"lquote'],
        ["''", None, "rspace", 'rquote"'],
        ["'hello'", None, "surround space", "surround quote"],
        ["hello", None, None, "surround quote and space"],
        ["hello you", "5", None, "surround quote and space"],
    ], columns=list("ABCD"))
    assert_frame_equal(df_under_test, expected_df)


def test_clean_frame_vectorized_mixed_types():
    df_under_test = pd.DataFrame([
        ['""', "", np.NaN, '"25'],
        ["''", " ", "NaN", '-10"'],
        ["'10'", "  ", np.int64(12), '"0"'],
        [77, "\n\t", None, 0.0],
        ['"11 8"', "5", np.float64(8.2), '99\t'],
    ], columns=list("ABCD"))

    df_under_test = validation_helper.clean_frame_vectorized(df_under_test, convert_to_str=True)

    expected_df = pd.DataFrame([
        [None, None, None, '"25'],
        ["''", None, "NaN", '-10"'],
        ["'10'", None, "12", "0"],
        ["77", None, None, "0.0"],
        ["11 8", "5", "8.2", "99"],
    ], columns=list("ABCD"))
    assert_frame_equal(df_under_test, expected_df)


def test_clean_numbers():
    # Normal cases
    assert validation_helper.clean_numbers('10') == '10'
    assert validation_helper.clean_numbers('1,00') == '100'
    assert validation_helper.clean_numbers('-10,000') == '-10000'
    assert validation_helper.clean_numbers('0') == '0'

    # This is originall designed for just strings but we should still account for these
    assert validation_helper.clean_numbers(10) == '10'
    assert validation_helper.clean_numbers(-10) == '-10'
    assert validation_helper.clean_numbers(0) == '0'
    assert validation_helper.clean_numbers(0) == '0'
    assert validation_helper.clean_numbers(None) is None
    assert validation_helper.clean_numbers(['A']) == ['A']


def test_clean_numbers_vectorized_all_strings():
    df_under_test = pd.DataFrame([
        ["10,003,234", "bad,and", "2242424242", "-10"],
        ["0", "8", "9.424.2", "-10,000"],
        ["9.24242", ",2,094", ",01", ",-0,0"],
        ["1,45", "0055", None, np.NaN]
    ], columns=list("ABCD"))

    for col in df_under_test.columns:
        validation_helper.clean_numbers_vectorized(df_under_test[col])

    expected_df = pd.DataFrame([
        ["10003234", "bad,and", "2242424242", "-10"],
        ["0", "8", "9.424.2", "-10000"],
        ["9.24242", "2094", "01", "-00"],
        ["145", "0055", None, np.NaN]
    ], columns=list("ABCD"))
    assert_frame_equal(df_under_test, expected_df)


def test_clean_numbers_vectorized_mixed_types():
    df_under_test = pd.DataFrame([
        ["10,003,234", "bad,and", 2242424242, -10],
        [0, 8, "9.424.2", -4.35],
        [9.24242, ",2,094", ",01", -0],
        ["1,45", "0055", None, np.NaN]
    ], columns=list("ABCD"))

    for col in df_under_test.columns:
        validation_helper.clean_numbers_vectorized(df_under_test[col], convert_to_str=True)

    expected_df = pd.DataFrame([
        ["10003234", "bad,and", 2242424242, -10],
        [0, 8, "9.424.2", -4.35],
        [9.24242, "2094", "01", -0],
        ["145", "0055", None, np.NaN]
    ], columns=list("ABCD"))
    assert_frame_equal(df_under_test, expected_df)


def test_concat_flex():
    # Tests a blank value, column sorting, ignoring row number, and the basic functionality
    flex_row = {'row_number': '4', 'col 3': None, 'col 2': 'B', 'col 1': 'A'}
    assert validation_helper.concat_flex(flex_row) == 'col 1: A, col 2: B, col 3: '

    flex_row = {'just one': 'column'}
    assert validation_helper.concat_flex(flex_row) == 'just one: column'


def test_derive_unique_id():
    row = {'display_tas': 'DISPLAY-TAS', 'afa_generated_unique': 'AFA-GENERATED-UNIQUE', 'something': 'else'}
    assert validation_helper.derive_unique_id(row, is_fabs=True) == 'AssistanceTransactionUniqueKey:' \
                                                                    ' AFA-GENERATED-UNIQUE'
    assert validation_helper.derive_unique_id(row, is_fabs=False) == 'TAS: DISPLAY-TAS'


def test_derive_fabs_awarding_sub_tier():
    row = {'awarding_sub_tier_agency_c': '9876', 'awarding_office_code': '4567'}
    derive_row = {'awarding_sub_tier_agency_c': None, 'awarding_office_code': '4567'}
    office_list = {'4567': '0123'}
    # Normal
    assert validation_helper.derive_fabs_awarding_sub_tier(row, office_list) == '9876'
    # Derivation
    assert validation_helper.derive_fabs_awarding_sub_tier(derive_row, office_list) == '0123'
    # Failed Derivation
    assert validation_helper.derive_fabs_awarding_sub_tier(derive_row, {}) is None


def test_derive_fabs_afa_generated_unique():
    # All populated
    row = {'awarding_sub_tier_agency_c': '0123',
           'fain': 'FAIN',
           'uri': 'URI',
           'assistance_listing_number': '4567',
           'award_modification_amendme': '0'}
    assert validation_helper.derive_fabs_afa_generated_unique(row) == '0123_FAIN_URI_4567_0'

    # Some missing
    row = {'awarding_sub_tier_agency_c': '0123',
           'fain': None,
           'uri': 'URI',
           'assistance_listing_number': '4567',
           'award_modification_amendme': None}
    assert validation_helper.derive_fabs_afa_generated_unique(row) == '0123_-none-_URI_4567_-none-'

    # All missing
    row = {'awarding_sub_tier_agency_c': None,
           'fain': None,
           'uri': None,
           'assistance_listing_number': None,
           'award_modification_amendme': None}
    assert validation_helper.derive_fabs_afa_generated_unique(row) == '-none-_-none-_-none-_-none-_-none-'


def test_derive_fabs_unique_award_key():
    # Record type 1 - choose URI
    row = {'awarding_sub_tier_agency_c': '0123',
           'fain': 'FAIN',
           'uri': 'URI',
           'record_type': '1'}
    assert validation_helper.derive_fabs_unique_award_key(row) == 'ASST_AGG_URI_0123'
    row = {'awarding_sub_tier_agency_c': None,
           'fain': 'FAIN',
           'uri': None,
           'record_type': '1'}
    assert validation_helper.derive_fabs_unique_award_key(row) == 'ASST_AGG_-NONE-_-NONE-'

    # Record type 2 - choose FAIN
    row = {'awarding_sub_tier_agency_c': '4567',
           'fain': 'FAIN',
           'uri': 'URI',
           'record_type': '2'}
    assert validation_helper.derive_fabs_unique_award_key(row) == 'ASST_NON_FAIN_4567'
    row = {'awarding_sub_tier_agency_c': None,
           'fain': None,
           'uri': 'URI',
           'record_type': '2'}
    assert validation_helper.derive_fabs_unique_award_key(row) == 'ASST_NON_-NONE-_-NONE-'


def test_apply_label():
    # Normal case
    labels = {'field_name': 'field_label'}
    row = {'Field Name': 'field_name'}
    assert validation_helper.apply_label(row, labels, is_fabs=True) == 'field_label'
    # Field name not in labels
    row = {'Field Name': 'other_field_name'}
    assert validation_helper.apply_label(row, labels, is_fabs=True) == ''
    # Not FABS
    row = {'Field Name': 'field_name'}
    assert validation_helper.apply_label(row, labels, is_fabs=False) == ''


def test_gather_flex_fields():
    flex_data = pd.DataFrame({'row_number': ['1', '2', '3', '4', '5'],
                              'concatted': ['A', 'B', 'C', 'D', 'E']})
    row = {'Row Number': '4'}
    assert validation_helper.gather_flex_fields(row, flex_data) == 'D'
    assert validation_helper.gather_flex_fields(row, None) == ''


def test_valid_type():
    str_field = FileColumn(field_types_id=FIELD_TYPE_DICT['STRING'])
    int_field = FileColumn(field_types_id=FIELD_TYPE_DICT['INT'])
    csv_schema = {'str_field': str_field, 'int_field': int_field}

    # For more detailed tests, see is_valid_type
    row = {'Field Name': 'int_field', 'Value Provided': 'this is a string'}
    assert validation_helper.valid_type(row, csv_schema) is False
    row = {'Field Name': 'int_field', 'Value Provided': '1000'}
    assert validation_helper.valid_type(row, csv_schema) is True


def test_expected_type():
    bool_field = FileColumn(field_types_id=FIELD_TYPE_DICT['BOOLEAN'])
    dec_field = FileColumn(field_types_id=FIELD_TYPE_DICT['DECIMAL'])
    csv_schema = {'bool_field': bool_field, 'dec_field': dec_field}

    row = {'Field Name': 'bool_field'}
    assert validation_helper.expected_type(row, csv_schema) == 'This field must be a boolean'
    row = {'Field Name': 'dec_field'}
    assert validation_helper.expected_type(row, csv_schema) == 'This field must be a decimal'


def test_valid_length():
    length_field = FileColumn(length=5)
    non_length_field = FileColumn()
    csv_schema = {'length_field': length_field, 'non_length_field': non_length_field}

    row = {'Field Name': 'length_field', 'Value Provided': 'this is more than five characters'}
    assert validation_helper.valid_length(row, csv_schema) is False
    row = {'Field Name': 'length_field', 'Value Provided': 'four'}
    assert validation_helper.valid_length(row, csv_schema) is True
    row = {'Field Name': 'non_length_field', 'Value Provided': 'can be any length'}
    assert validation_helper.valid_length(row, csv_schema) is True


def test_expected_length():
    length_field = FileColumn(length=5)
    non_length_field = FileColumn()
    csv_schema = {'length_field': length_field, 'non_length_field': non_length_field}

    row = {'Field Name': 'length_field'}
    assert validation_helper.expected_length(row, csv_schema) == 'Max length: 5'
    row = {'Field Name': 'non_length_field'}
    assert validation_helper.expected_length(row, csv_schema) == 'Max length: None'


def test_update_field_name():
    short_cols = {'short_field_name': 'sfn'}

    row = {'Field Name': 'short_field_name'}
    assert validation_helper.update_field_name(row, short_cols) == 'sfn'
    row = {'Field Name': 'long_field_name'}
    assert validation_helper.update_field_name(row, short_cols) == 'long_field_name'


def test_add_field_name_to_value():
    row = {'Field Name': 'field_name', 'Value Provided': 'value_provided'}
    assert validation_helper.add_field_name_to_value(row) == 'field_name: value_provided'


def test_check_required():
    data = pd.DataFrame({'row_number': ['1', '2', '3', '4', '5'],
                         'unique_id': ['ID1', 'ID2', 'ID3', 'ID4', 'ID5'],
                         'required1': ['Yes', 'Yes', None, 'Yes', None],
                         'required2': ['Yes', None, 'Yes', None, None],
                         'not_required1': [None, None, 'Yes', None, None],
                         'not_required2': ['Yes', 'Yes', None, 'Yes', 'Yes']})
    required = ['required1', 'required2']
    required_labels = {'required1': 'Required 1'}
    report_headers = ValidationManager.report_headers
    short_cols = {'required1': 'req1', 'not_required1': 'n_req1', 'not_required2': 'n_req2'}
    flex_data = pd.DataFrame({'row_number': ['1', '2', '3', '4', '5'],
                              'concatted': ['A', 'B', 'C', 'D', 'E']})
    is_fabs = False

    error_msg = ValidationError.requiredErrorMsg
    expected_value = '(not blank)'
    error_type = ValidationError.requiredError
    # report_headers = ['Unique ID', 'Field Name', 'Rule Message', 'Value Provided', 'Expected Value', 'Difference',
    #                   'Flex Field', 'Row Number', 'Rule Label'] + ['error_type']
    expected_data = [
        ['ID3', 'req1', error_msg, '', expected_value, '', 'C', '3', '', error_type],
        ['ID5', 'req1', error_msg, '', expected_value, '', 'E', '5', '', error_type],
        ['ID2', 'required2', error_msg, '', expected_value, '', 'B', '2', '', error_type],
        ['ID4', 'required2', error_msg, '', expected_value, '', 'D', '4', '', error_type],
        ['ID5', 'required2', error_msg, '', expected_value, '', 'E', '5', '', error_type]
    ]
    expected_error_df = pd.DataFrame(expected_data, columns=report_headers + ['error_type'])
    error_df = validation_helper.check_required(data, required, required_labels, report_headers, short_cols, flex_data,
                                                is_fabs)
    assert_frame_equal(error_df, expected_error_df)

    is_fabs = True
    expected_data = [
        ['ID3', 'req1', error_msg, '', expected_value, '', 'C', '3', 'Required 1', error_type],
        ['ID5', 'req1', error_msg, '', expected_value, '', 'E', '5', 'Required 1', error_type],
        ['ID2', 'required2', error_msg, '', expected_value, '', 'B', '2', '', error_type],
        ['ID4', 'required2', error_msg, '', expected_value, '', 'D', '4', '', error_type],
        ['ID5', 'required2', error_msg, '', expected_value, '', 'E', '5', '', error_type]
    ]
    expected_error_df = pd.DataFrame(expected_data, columns=report_headers + ['error_type'])
    error_df = validation_helper.check_required(data, required, required_labels, report_headers, short_cols, flex_data,
                                                is_fabs)
    assert_frame_equal(error_df, expected_error_df)


def test_check_type():
    data = pd.DataFrame({'row_number': ['1', '2', '3', '4', '5'],
                         'unique_id': ['ID1', 'ID2', 'ID3', 'ID4', 'ID5'],
                         'int': ['1', '2', '3', 'no', '5'],
                         'dec': ['1.3', '1', 'no', '1232', '4.3'],
                         'bool': ['no', 'Yes', 'TRUE', 'false', '4'],
                         'string': ['this', 'row', 'should', 'be', 'ignored']})
    type_fields = ['int', 'bool', 'dec']
    type_labels = {'int': 'Integer', 'dec': 'Decimal'}
    report_headers = ValidationManager.report_headers
    csv_schema = {'int': FileColumn(field_types_id=FIELD_TYPE_DICT['INT']),
                  'bool': FileColumn(field_types_id=FIELD_TYPE_DICT['BOOLEAN']),
                  'dec': FileColumn(field_types_id=FIELD_TYPE_DICT['DECIMAL'])}
    short_cols = {'int': 'i', 'bool': 'b'}
    flex_data = pd.DataFrame({'row_number': ['1', '2', '3', '4', '5'],
                              'concatted': ['A', 'B', 'C', 'D', 'E']})
    is_fabs = False

    error_msg = ValidationError.typeErrorMsg
    error_type = ValidationError.typeError
    # report_headers = ['Unique ID', 'Field Name', 'Rule Message', 'Value Provided', 'Expected Value', 'Difference',
    #                   'Flex Field', 'Row Number', 'Rule Label'] + ['error_type']
    expected_data = [
        ['ID4', 'i', error_msg, 'i: no', 'This field must be a int', '', 'D', '4', '', error_type],
        ['ID5', 'b', error_msg, 'b: 4', 'This field must be a boolean', '', 'E', '5', '', error_type],
        ['ID3', 'dec', error_msg, 'dec: no', 'This field must be a decimal', '', 'C', '3', '', error_type]
    ]
    expected_error_df = pd.DataFrame(expected_data, columns=report_headers + ['error_type'])
    error_df = validation_helper.check_type(data, type_fields, type_labels, report_headers, csv_schema, short_cols,
                                            flex_data, is_fabs)
    assert_frame_equal(error_df, expected_error_df)

    is_fabs = True
    expected_data = [
        ['ID4', 'i', error_msg, 'i: no', 'This field must be a int', '', 'D', '4', 'Integer', error_type],
        ['ID5', 'b', error_msg, 'b: 4', 'This field must be a boolean', '', 'E', '5', '', error_type],
        ['ID3', 'dec', error_msg, 'dec: no', 'This field must be a decimal', '', 'C', '3', 'Decimal', error_type]
    ]
    expected_error_df = pd.DataFrame(expected_data, columns=report_headers + ['error_type'])
    error_df = validation_helper.check_type(data, type_fields, type_labels, report_headers, csv_schema, short_cols,
                                            flex_data, is_fabs)
    assert_frame_equal(error_df, expected_error_df)


def test_check_length():
    data = pd.DataFrame({'row_number': ['1', '2', '3', '4', '5'],
                         'unique_id': ['ID1', 'ID2', 'ID3', 'ID4', 'ID5'],
                         'has_length': ['1', '12', '123', '1234', '12345'],
                         'no_length': ['', '1', 'no', '1232', '4.3']})
    length_fields = ['has_length']
    report_headers = ValidationManager.report_headers
    csv_schema = {'has_length': FileColumn(length=3),
                  'no_length': FileColumn()}
    short_cols = {'has_length': 'len'}
    flex_data = pd.DataFrame({'row_number': ['1', '2', '3', '4', '5'],
                              'concatted': ['A', 'B', 'C', 'D', 'E']})
    type_error_rows = ['5']

    error_msg = ValidationError.lengthErrorMsg
    error_type = ValidationError.lengthError
    # report_headers = ['Unique ID', 'Field Name', 'Rule Message', 'Value Provided', 'Expected Value', 'Difference',
    #                   'Flex Field', 'Row Number', 'Rule Label'] + ['error_type']
    expected_data = [
        ['ID4', 'len', error_msg, 'len: 1234', 'Max length: 3', '', 'D', '4', '', error_type]
    ]
    expected_error_df = pd.DataFrame(expected_data, columns=report_headers + ['error_type'])
    error_df = validation_helper.check_length(data, length_fields, report_headers, csv_schema, short_cols, flex_data,
                                              type_error_rows)
    assert_frame_equal(error_df, expected_error_df)


def test_check_field_format():
    data = pd.DataFrame({'row_number': ['1', '2', '3', '4', '5'],
                         'unique_id': ['ID1', 'ID2', 'ID3', 'ID4', 'ID5'],
                         'dates': [None, '20200101', '200012', 'abcdefgh', '20201301']})
    format_fields = ['dates']
    report_headers = ValidationManager.report_headers
    short_cols = {'dates': 'date'}
    flex_data = pd.DataFrame({'row_number': ['1', '2', '3', '4', '5'],
                              'concatted': ['A', 'B', 'C', 'D', 'E']})

    error_msg = ValidationError.fieldFormatErrorMsg
    error_type = ValidationError.fieldFormatError
    # report_headers = ['Unique ID', 'Field Name', 'Rule Message', 'Value Provided', 'Expected Value', 'Difference',
    #                   'Flex Field', 'Row Number', 'Rule Label'] + ['error_type']
    expected_data = [
        ['ID3', 'date', error_msg, 'date: 200012', 'A date in the YYYYMMDD format.', '', 'C', '3', 'DABSDATETIME',
         error_type],
        ['ID4', 'date', error_msg, 'date: abcdefgh', 'A date in the YYYYMMDD format.', '', 'D', '4', 'DABSDATETIME',
         error_type],
        ['ID5', 'date', error_msg, 'date: 20201301', 'A date in the YYYYMMDD format.', '', 'E', '5', 'DABSDATETIME',
         error_type]
    ]
    expected_error_df = pd.DataFrame(expected_data, columns=report_headers + ['error_type'])
    error_df = validation_helper.check_field_format(data, format_fields, report_headers, short_cols, flex_data)
    assert_frame_equal(error_df, expected_error_df)


def test_parse_fields(database):
    sess = database.session
    fields = [
        FileColumn(name_short='string', field_types_id=FIELD_TYPE_DICT['STRING'], length=5),
        FileColumn(name_short='bool', field_types_id=FIELD_TYPE_DICT['BOOLEAN'], required=True),
        FileColumn(name_short='dec', field_types_id=FIELD_TYPE_DICT['DECIMAL']),
        FileColumn(name_short='int', field_types_id=FIELD_TYPE_DICT['INT'], padded_flag=True, length=4, required=True),
        FileColumn(name_short='date', field_types_id=FIELD_TYPE_DICT['DATE'])
    ]
    sess.add_all(fields)

    expected_parsed_fields = {
        'required': ['bool', 'int'],
        'number': ['dec', 'int'],
        'boolean': ['bool'],
        'format': ['date'],
        'length': ['string', 'int'],
        'padded': ['int']
    }
    expected_expected_headers = ['bool', 'int', 'dec', 'string', 'date']
    expected_headers, parsed_fields = validation_helper.parse_fields(sess, fields)
    assert parsed_fields == expected_parsed_fields
    assert set(expected_headers) == set(expected_expected_headers)


def test_process_formatting_errors():
    short_rows = ['1', '4', '5']
    long_rows = ['2', '6']

    error_msg = ValidationError.readErrorMsg
    error_type = ValidationError.readError
    error_name = 'Formatting Error'
    report_headers = ValidationManager.report_headers
    expected_data = [
        ['', error_name, error_msg, '', '', '', '', '1', '', error_type],
        ['', error_name, error_msg, '', '', '', '', '2', '', error_type],
        ['', error_name, error_msg, '', '', '', '', '4', '', error_type],
        ['', error_name, error_msg, '', '', '', '', '5', '', error_type],
        ['', error_name, error_msg, '', '', '', '', '6', '', error_type]
    ]
    expected_format_error_df = pd.DataFrame(expected_data, columns=report_headers + ['error_type'])
    assert_frame_equal(validation_helper.process_formatting_errors(short_rows, long_rows, report_headers),
                       expected_format_error_df)


def test_simple_file_scan():
    # Note: only testing locally
    assert validation_helper.simple_file_scan(CsvReader(), None, None, READ_ERROR) == (11, [5], [2, 3, 7], [], [])
    assert validation_helper.simple_file_scan(CsvReader(), None, None, BLANK_C) == (5, [], [], [3], [4])


@pytest.mark.usefixtures("job_constants")
def test_update_val_progress(database):
    sess = database.session
    sub = SubmissionFactory(submission_id=1)
    fabs_sub = SubmissionFactory(submission_id=2, is_fabs=True)
    job = JobFactory(submission=sub, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'],
                     progress=32.4)
    fabs_job = JobFactory(submission=fabs_sub, job_status_id=JOB_STATUS_DICT['finished'],
                          job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['fabs'],
                          progress=0)
    sess.add_all([sub, fabs_sub, job, fabs_job])
    sess.commit()

    validation_helper.update_val_progress(sess, job, 100, 100, 25, 0)
    assert job.progress == 57.5

    validation_helper.update_val_progress(sess, fabs_job, 100, 0, 4.5, 0)
    assert fabs_job.progress == 42.25


@pytest.mark.usefixtures("job_constants")
def test_update_cross_val_progress(database):
    sess = database.session
    sub = SubmissionFactory(submission_id=1)
    job = JobFactory(submission=sub, job_status_id=JOB_STATUS_DICT['running'],
                     job_type_id=JOB_TYPE_DICT['validation'], file_type_id=None,
                     progress=32.4)
    sess.add_all([sub, job])
    sess.commit()

    validation_helper.update_cross_val_progress(sess, job, 2, 4, 1)
    assert job.progress == 56.25
