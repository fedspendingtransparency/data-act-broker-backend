from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import CFDAProgram
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs37_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'cfda_number', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test that no errors occur when the AssistanceListingNumber exists. """

    cfda = CFDAProgram(program_number=12.340)
    fabs_1 = FABSFactory(cfda_number='12.340', correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    fabs_2 = FABSFactory(cfda_number='AB.CDE', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, cfda])
    assert errors == 0


def test_failure(database):
    """ Test that its fails when AssistanceListingNumber does not exists. """

    # test for cfda_number that doesn't exist in the table
    cfda = CFDAProgram(program_number=12.340)
    fabs_1 = FABSFactory(cfda_number='54.321', correction_delete_indicatr='')
    fabs_2 = FABSFactory(cfda_number='AB.CDE', correction_delete_indicatr='c')
    fabs_3 = FABSFactory(cfda_number='11.111', correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, cfda])
    assert errors == 3
