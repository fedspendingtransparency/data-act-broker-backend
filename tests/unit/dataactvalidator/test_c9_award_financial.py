from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c9_award_financial'
_TAS = 'c9_award_financial_tas'


def test_column_headers(database):
    expected_subset = {'row_number', "fain", "uri"}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_equal_fain(database):
    """ Tests that File D2 (award financial assistance) fain matches File C (award financial) fain. """
    tas = _TAS
    afa = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=1,
                                          original_loan_subsidy_cost='1')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=afa.fain, uri=None)

    errors = number_of_errors(_FILE, database, models=[afa, af])
    assert errors == 0


def test_equal_uri(database):
    """ Tests that File D2 (award financial assistance) uri matches File C (award financial) uri. """
    tas = _TAS
    afa = AwardFinancialAssistanceFactory(tas=tas, fain=None, uri='xyz', federal_action_obligation=1,
                                          original_loan_subsidy_cost='1')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=None, uri=afa.uri)

    errors = number_of_errors(_FILE, database, models=[afa, af])
    assert errors == 0


def test_null_uri_fain(database):
    """ Tests File D2 (award financial assistance) and File C (award financial)
        having NULL values for both fain and uri. """
    tas = _TAS
    afa = AwardFinancialAssistanceFactory(tas=tas, fain=None, uri=None, federal_action_obligation=1,
                                          original_loan_subsidy_cost='1')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=None, uri=None)

    errors = number_of_errors(_FILE, database, models=[afa, af])
    assert errors == 0


def test_both_fain_and_url_supplied(database):
    """ Tests File D2 (award financial assistance) having both uri and fain populated. """
    tas = _TAS
    afa = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri='xyz', federal_action_obligation=1,
                                          original_loan_subsidy_cost='1')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=afa.fain, uri=afa.uri)

    errors = number_of_errors(_FILE, database, models=[afa, af])
    assert errors == 0


def test_unequal_fain(database):
    """ Tests File D2 (award financial assistance) fain different than File C (award financial) fain. """
    tas = _TAS
    afa = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=1,
                                          original_loan_subsidy_cost='1')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain='xyz', uri=None)

    errors = number_of_errors(_FILE, database, models=[afa, af])
    assert errors == 1


def test_unequal_uri(database):
    """ Tests File D2 (award financial assistance) uri different than File C (award financial) uri. """
    tas = _TAS
    afa = AwardFinancialAssistanceFactory(tas=tas, fain=None, uri='abc', federal_action_obligation=1,
                                          original_loan_subsidy_cost='1')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=None, uri='xyz')

    errors = number_of_errors(_FILE, database, models=[afa, af])
    assert errors == 1


def test_unequal_fain_null(database):
    """Tests non-NULL File D2 (award financial assistance) fain compared to a NULL fain in File C (award financial)."""
    tas = _TAS
    afa = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=1,
                                          original_loan_subsidy_cost='1')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=None, uri=None)

    errors = number_of_errors(_FILE, database, models=[afa, af])
    assert errors == 1


def test_unequal_uri_null(database):
    """ Tests NULL File D2 (award financial assistance) uri compared to a non-NULL uri in File C (award financial). """
    tas = _TAS
    afa = AwardFinancialAssistanceFactory(tas=tas, fain=None, uri=None, federal_action_obligation=1,
                                          original_loan_subsidy_cost='1')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=None, uri='abc')

    errors = number_of_errors(_FILE, database, models=[afa, af])
    assert errors == 0


def test_zero_federal_action_obligation_and_original_loan_subsidy_cost(database):
    """ Tests that a single warning is thrown for both a federal action obligation of 0
        and an original loan subsidy cost of 0 """
    tas = _TAS
    afa = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=0,
                                          original_loan_subsidy_cost='0')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=None, uri=None)

    errors = number_of_errors(_FILE, database, models=[afa, af])
    assert errors == 0


def test_ignored_and_failed_federal_action_obligation_values(database):
    """ Tests that a single warning is thrown for both a federal action obligation of 0
        and an original loan subsidy cost of 0 """

    tas = _TAS
    afa = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=0,
                                          original_loan_subsidy_cost='1', assistance_type='08')
    afa_2 = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=2,
                                            original_loan_subsidy_cost='1', assistance_type='07')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=None, uri=None)

    errors = number_of_errors(_FILE, database, models=[afa, af, afa_2])
    assert errors == 2

    # Test that this is ignored if assistance type is 07
    afa = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=0,
                                          original_loan_subsidy_cost='1', assistance_type='07')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=None, uri=None)

    errors = number_of_errors(_FILE, database, models=[afa, af])
    assert errors == 0


def test_ignored_and_failed_original_loan_subsidy_cost_values(database):
    """ Tests that a single warning is thrown for both a federal action obligation of 0
        and an original loan subsidy cost of 0 """

    tas = _TAS
    afa = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=1,
                                          original_loan_subsidy_cost='0', assistance_type='07')
    afa_2 = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=1,
                                            original_loan_subsidy_cost='-2.3', assistance_type='07')
    afa_3 = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=1,
                                            original_loan_subsidy_cost='2.3', assistance_type='08')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=None, uri=None)

    errors = number_of_errors(_FILE, database, models=[afa, af, afa_2, afa_3])
    assert errors == 3

    # Test that this is ignored if assistance type is 08
    afa = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=1,
                                          original_loan_subsidy_cost='0', assistance_type='08')
    afa_2 = AwardFinancialAssistanceFactory(tas=tas, fain='abc', uri=None, federal_action_obligation=1,
                                            original_loan_subsidy_cost='-2.3', assistance_type='08')
    af = AwardFinancialFactory(tas=tas, submisson_id=afa.submission_id, fain=None, uri=None)

    errors = number_of_errors(_FILE, database, models=[afa, af, afa_2])
    assert errors == 0
