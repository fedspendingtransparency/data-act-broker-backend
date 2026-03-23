from tests.unit.dataactcore.factories.domain import ProgramActivityPARKFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "b28_award_financial"


def test_column_headers(database):
    expected_subset = {"row_number", "program_activity_reporting_key", "uniqueid_TAS", "uniqueid_ObjectClass"}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """
    Should be a valid ProgramActivityReportingKey (PARK) for the corresponding funding TAS/TAFS, as defined in the
    OMB’s Program Activity Mapping File. Ignore rule for $0 rows IF PARK is 0 or 0000
    """

    park = ProgramActivityPARKFactory(
        agency_id="123",
        allocation_transfer_id=None,
        main_account_number="0001",
        sub_account_number=None,
        park_code="abcd",
    )
    park_sub = ProgramActivityPARKFactory(
        agency_id="123",
        allocation_transfer_id=None,
        main_account_number="0002",
        sub_account_number="001",
        park_code="AbCdEf",
    )

    # Main account code that has no sub in PARK, sub ignored
    af1 = AwardFinancialFactory(
        agency_identifier="123",
        allocation_transfer_agency=None,
        main_account_code="0001",
        sub_account_code="123",
        program_activity_reporting_key="aBcD",
    )
    # Matching main and sub accounts
    af2 = AwardFinancialFactory(
        agency_identifier="123",
        allocation_transfer_agency=None,
        main_account_code="0002",
        sub_account_code="001",
        program_activity_reporting_key="aBcDeF",
    )
    # Ignored for NULL PARK
    af3 = AwardFinancialFactory(
        agency_identifier="123",
        allocation_transfer_agency=None,
        main_account_code="0002",
        sub_account_code="003",
        program_activity_reporting_key=None,
    )
    assert number_of_errors(_FILE, database, models=[af1, af2, af3, park, park_sub]) == 0


def test_failure(database):
    """
    Failure should be a valid ProgramActivityReportingKey (PARK) for the corresponding funding TAS/TAFS, as defined
    in the OMB’s Program Activity Mapping File. Ignore rule for $0 rows IF PARK is 0 or 0000
    """

    park = ProgramActivityPARKFactory(
        agency_id="123",
        allocation_transfer_id=None,
        main_account_number="0001",
        sub_account_number=None,
        park_code="ABCD",
    )
    park_sub = ProgramActivityPARKFactory(
        agency_id="123",
        allocation_transfer_id=None,
        main_account_number="0002",
        sub_account_number="001",
        park_code="ABCDEF",
    )
    # Non-matching sub account code
    af1 = AwardFinancialFactory(
        agency_identifier="123",
        allocation_transfer_agency=None,
        main_account_code="0002",
        sub_account_code="123",
        program_activity_reporting_key="ABCDEF",
    )
    # Non-matching TAS even though PARK exists
    af2 = AwardFinancialFactory(
        agency_identifier="321",
        allocation_transfer_agency=None,
        main_account_code="0001",
        sub_account_code="123",
        program_activity_reporting_key="ABCD",
    )
    # PARK that doesn't exist even though TAS does
    af3 = AwardFinancialFactory(
        agency_identifier="123",
        allocation_transfer_agency=None,
        main_account_code="0001",
        sub_account_code="123",
        program_activity_reporting_key="ABCDE",
    )

    assert number_of_errors(_FILE, database, models=[af1, af2, af3, park, park_sub]) == 3
