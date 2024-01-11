from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import CFDAProgram
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs37_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'assistance_listing_number', 'action_date', 'action_type',
                       'correction_delete_indicatr', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ Test valid. For (ActionType = B, C, or D), the AssistanceListingNumber need NOT be active as of the ActionDate.
        Not apply to those with CorrectionDeleteIndicator = C.
        Active date: publish_date <= action_date <= archive_date (Fails validation if active).
    """

    cfda = CFDAProgram(program_number=12.340, published_date='20130427', archived_date='')
    fabs_1 = FABSFactory(cfda_number='12.340', action_date='20140528', action_type='b', correction_delete_indicatr='B')
    fabs_2 = FABSFactory(cfda_number='12.340', action_date='20140428', action_type='c', correction_delete_indicatr='')
    fabs_3 = FABSFactory(cfda_number='12.340', action_date='20140428', action_type='D', correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(cfda_number='12.340', action_date='20120528', action_type='B', correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, cfda])
    assert errors == 0

    cfda = CFDAProgram(program_number=12.350, published_date='20130427', archived_date='20140427')
    fabs_1 = FABSFactory(cfda_number='12.350', action_date='20130528', action_type='b', correction_delete_indicatr='B')
    fabs_2 = FABSFactory(cfda_number='12.350', action_date='20130428', action_type='C', correction_delete_indicatr='')
    fabs_3 = FABSFactory(cfda_number='12.350', action_date='20130428', action_type='d', correction_delete_indicatr=None)
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, cfda])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test invalid. For (ActionType = B, C, or D), the AssistanceListingNumber need NOT be active as of the ActionDate
        Not apply to those with CorrectionDeleteIndicator = C.
        Active date: publish_date <= action_date <= archive_date (Fails validation if active).
        If action date is < published_date, should trigger a warning.
    """

    cfda = CFDAProgram(program_number=12.340, published_date='20130427', archived_date='')
    fabs_1 = FABSFactory(cfda_number='12.340', action_date='20120528', action_type='b', correction_delete_indicatr='B')
    fabs_2 = FABSFactory(cfda_number='12.340', action_date='20120427', action_type='C', correction_delete_indicatr='')
    fabs_3 = FABSFactory(cfda_number='12.340', action_date='20120428', action_type='d', correction_delete_indicatr=None)
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, cfda])
    assert errors == 3

    cfda = CFDAProgram(program_number=12.350, published_date='20130427', archived_date='20140528')
    fabs_1 = FABSFactory(cfda_number='12.350', action_date='20120528', action_type='B', correction_delete_indicatr='B')
    fabs_2 = FABSFactory(cfda_number='12.350', action_date='20150427', action_type='c', correction_delete_indicatr='')
    fabs_3 = FABSFactory(cfda_number='12.350', action_date='20150428', action_type='D', correction_delete_indicatr=None)
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, cfda])
    assert errors == 3
