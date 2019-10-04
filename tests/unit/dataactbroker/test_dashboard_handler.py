import json
from datetime import datetime

from dataactbroker.helpers.generic_helper import fy
from dataactbroker.handlers import dashboard_handler
from dataactcore.utils.responseException import ResponseException

def historic_dabs_warning_summary_endpoint(filters):
    json_response = dashboard_handler.historic_dabs_warning_summary(filters)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))

def test_validate_historic_dashboard_filters():
    def assert_validation(filters, expected_response):
        try:
            dashboard_handler.validate_historic_dashboard_filters(filters)
        except ResponseException as e:
            assert str(e) == expected_response

    # missing a required filter
    assert_validation({'quarters': [], 'fys': []}, 'The following filters were not provided: agencies')

    # not a list
    filters = {'quarters': [1], 'fys': 'not a list', 'agencies': ['097']}
    assert_validation(filters, 'The following filters were not lists: fys')

    # wrong quarters
    error_message = 'Quarters must be a list of integers, each ranging 1-4, or an empty list.'
    filters = {'quarters': [5], 'fys': [2017, 2019], 'agencies': ['097']}
    assert_validation(filters, error_message)
    filters['quarters'] = ['3']
    assert_validation(filters, error_message)

    # wrong fys
    current_fy = fy(datetime.now())
    error_message = 'Fiscal Years must be a list of integers, each ranging from 2017 through the current fiscal year,'\
                    ' or an empty list.'
    filters = {'quarters': [1, 3], 'fys': [2016, 2019], 'agencies': ['097']}
    assert_validation(filters, error_message)
    filters = {'quarters': [1, 3], 'fys': [2017, current_fy+1], 'agencies': ['097']}
    assert_validation(filters, error_message)
    filters = {'quarters': [1, 3], 'fys': [2017, str(current_fy)], 'agencies': ['097']}
    assert_validation(filters, error_message)

    # wrong agencies
    filters = {'quarters': [1, 3], 'fys': [2017, 2019], 'agencies': [97]}
    assert_validation(filters, 'Agencies must be a list of strings, or an empty list.')


# def test_historic_dabs_warning_summary():
#     filters = {
#         'quarters': [],
#         'fys': [],
#         'agencies': []
#     }
