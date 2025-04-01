import pytest
from datetime import datetime
from dateutil.relativedelta import relativedelta

from dataactbroker.helpers import script_helper

from dataactcore.interfaces.function_bag import get_utc_now
from dataactcore.models.domainModels import ExternalDataLoadDate
from dataactcore.models.lookups import EXTERNAL_DATA_TYPE_DICT


def test_validate_load_dates(database):
    """ Test validate_load_dates """
    sess = database.session

    load_type = None
    # Default arg date format
    arg_date_format, output_date_format = '%m/%d/%Y', '%m/%d/%Y'

    # Error Checks
    auto, start_date, end_date = False, None, None
    expected_error_text = 'start_date, end_date, or auto setting is required.'
    with pytest.raises(ValueError) as except_error:
        script_helper.validate_load_dates(start_date, end_date, auto, load_type, arg_date_format, output_date_format)
    assert str(except_error.value) == expected_error_text

    auto, start_date, end_date = False, ['01/02/2000'], ['01/01/2000']
    expected_error_text = 'Start date cannot be later than end date.'
    with pytest.raises(ValueError) as except_error:
        script_helper.validate_load_dates(start_date, end_date, auto, load_type, arg_date_format, output_date_format)
    assert str(except_error.value) == expected_error_text

    # Formatting Checks
    auto, start_date, end_date = False, ['2000-01-01'], ['01/02/2000']
    expected_error_text = 'time data \'2000-01-01\' does not match format \'%m/%d/%Y\''
    with pytest.raises(ValueError) as except_error:
        script_helper.validate_load_dates(start_date, end_date, auto, load_type, arg_date_format, output_date_format)
    assert str(except_error.value) == expected_error_text

    auto, start_date, end_date = False, ['01/01/2000'], ['2000-01-02']
    expected_error_text = 'time data \'2000-01-02\' does not match format \'%m/%d/%Y\''
    with pytest.raises(ValueError) as except_error:
        script_helper.validate_load_dates(start_date, end_date, auto, load_type, arg_date_format, output_date_format)
    assert str(except_error.value) == expected_error_text

    auto, start_date, end_date = False, ['01/01/2000'], ['01/02/2000']
    output_date_format = '%Y-%m-%d'
    start_date, end_date = script_helper.validate_load_dates(start_date, end_date, auto, load_type, arg_date_format,
                                                             output_date_format)
    assert start_date == '2000-01-01'
    assert end_date == '2000-01-03'

    # Auto Check
    load_type = 'office'
    today = get_utc_now()
    yesterday = today.date() - relativedelta(days=1)
    auto, start_date, end_date = True, None, None

    # One without a previous load date
    sess.query(ExternalDataLoadDate). \
        filter_by(external_data_type_id=EXTERNAL_DATA_TYPE_DICT[load_type]).delete()
    sess.commit()
    start_date, end_date = script_helper.validate_load_dates(start_date, end_date, auto, load_type, arg_date_format,
                                                             output_date_format)
    assert start_date == yesterday.strftime(output_date_format)
    assert end_date is None

    # One with a previous load date
    auto, start_date, end_date = True, None, None
    loader_date = datetime.strptime('2000-01-01', '%Y-%m-%d').date()
    sess.add(ExternalDataLoadDate(external_data_type_id=EXTERNAL_DATA_TYPE_DICT[load_type],
                                  last_load_date_start=loader_date))
    sess.commit()
    start_date, end_date = script_helper.validate_load_dates(start_date, end_date, auto, load_type, arg_date_format,
                                                             output_date_format)
    assert start_date == loader_date.strftime(output_date_format)
    assert end_date is None


def test_flatten_json():
    """ Test flattening jsons """
    test_json = {'a': [1, 2, 3], 'b': [2, 3, 4]}
    result = {'a_0': 1, 'a_1': 2, 'a_2': 3, 'b_0': 2, 'b_1': 3, 'b_2': 4}
    assert script_helper.flatten_json(test_json) == result

    test_json = [{'a': [1, 2, 3]}, {'b': [2, 3, 4]}]
    result = {'0_a_0': 1, '0_a_1': 2, '0_a_2': 3, '1_b_0': 2, '1_b_1': 3, '1_b_2': 4}
    assert script_helper.flatten_json(test_json) == result


def test_trim_nested_obj():
    """ Test trimming nested objects """
    test_json = [{' a ': ['1', '         2', '3     '], 'b': ['  2   ', '  3', '4']}]
    # note: it doesn't trim the keys, only the values
    result = [{' a ': ['1', '2', '3'], 'b': ['2', '3', '4']}]
    assert script_helper.trim_nested_obj(test_json) == result
