from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs18'


def test_column_headers(database):
    expected_subset = {'row_number', 'business_types', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ BusinessTypes must be one to three letters in length. BusinessTypes values must be non-repeated letters
        from A to X.
    """
    fabs = FABSFactory(business_types='A', correction_delete_indicatr='')
    fabs_2 = FABSFactory(business_types='XB', correction_delete_indicatr=None)
    fabs_3 = FABSFactory(business_types='RCm', correction_delete_indicatr='c')
    fabs_4 = FABSFactory(business_types='rcm', correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    fabs_5 = FABSFactory(business_types='BOB', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """ BusinessTypes must be one to three letters in length. BusinessTypes values must be non-repeated letters
        from A to X.
    """

    # Test if it's somehow empty or has 4 letters (length test)
    fabs = FABSFactory(business_types='', correction_delete_indicatr='')
    fabs_2 = FABSFactory(business_types='ABCD', correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2

    # Test repeats
    fabs = FABSFactory(business_types='BOb', correction_delete_indicatr='')
    fabs_2 = FABSFactory(business_types='BOB', correction_delete_indicatr='c')
    fabs_3 = FABSFactory(business_types='BbO', correction_delete_indicatr='')
    fabs_4 = FABSFactory(business_types='BB', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4])
    assert errors == 4

    # Test that only valid letters work
    fabs = FABSFactory(business_types='ABY', correction_delete_indicatr='')
    fabs_2 = FABSFactory(business_types='C2', correction_delete_indicatr='c')
    fabs_3 = FABSFactory(business_types='c2d', correction_delete_indicatr='')
    fabs_4 = FABSFactory(business_types='123', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4])
    assert errors == 4
