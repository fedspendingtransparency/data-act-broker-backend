"""Test file for the three version of the A1 rule. We abstract away the
differences into parametrized pytests"""
import pytest

from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.staging import (
    AppropriationFactory, AwardFinancialFactory, ObjectClassProgramActivityFactory
)
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_factories = {
    'a1_appropriations': AppropriationFactory,
    'a1_award_financial': AwardFinancialFactory,
    'a1_object_class_program_activity': ObjectClassProgramActivityFactory
}


@pytest.mark.parametrize('sql_file', _factories.keys())
def test_column_headers(database, sql_file):
    expected_subset = {
        'uniqueid_TAS', 'row_number', 'allocation_transfer_agency', 'agency_identifier',
        'beginning_period_of_availa', 'ending_period_of_availabil',
        'availability_type_code', 'main_account_code', 'sub_account_code'}

    actual = set(query_columns(sql_file, database))
    assert expected_subset == actual


@pytest.mark.parametrize('sql_file,factory', _factories.items())
def test_success(database, sql_file, factory):
    """If a TAS value is set, we should succeed"""
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()
    model = factory(tas_id=tas.tas_id)

    assert number_of_errors(sql_file, database, models=[tas, model]) == 0


@pytest.mark.parametrize('sql_file,factory', _factories.items())
def test_failure(database, sql_file, factory):
    """If no TAS value is set, we fail"""
    tas = TASFactory()
    model = factory(tas_id=None)
    assert number_of_errors(sql_file, database, models=[tas, model]) == 1
