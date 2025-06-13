from tests.unit.dataactcore.factories.domain import OfficeFactory
from tests.unit.dataactcore.factories.staging import FABSFactory, PublishedFABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs38_4_2"


def test_column_headers(database):
    expected_subset = {"row_number", "awarding_office_code", "action_date", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success_ignore_null_pub_fabs(database):
    """Test that empty awarding office codes aren't matching invalid office codes from the base record."""

    office = OfficeFactory(
        office_code="12345a",
        financial_assistance_awards_office=True,
        effective_start_date="01/01/2018",
        effective_end_date=None,
    )
    # Base record has no awarding office code, future records don't affect it
    pub_fabs_1 = PublishedFABSFactory(
        awarding_office_code="",
        unique_award_key="zyxwv_123",
        action_date="20181018",
        award_modification_amendme="0",
        is_active=True,
    )
    pub_fabs_2 = PublishedFABSFactory(
        awarding_office_code="abc",
        unique_award_key="zyxwv_123",
        action_date="20181019",
        award_modification_amendme="1",
        is_active=True,
    )
    # Base record has an invalid code but new record has a awarding office entered (ignore this rule)
    pub_fabs_3 = PublishedFABSFactory(
        awarding_office_code="abc",
        unique_award_key="abcd_123",
        action_date="20181019",
        award_modification_amendme="0",
        is_active=True,
    )
    # Base record with a valid office code (case insensitive)
    pub_fabs_4 = PublishedFABSFactory(
        awarding_office_code="12345A",
        unique_award_key="1234_abc",
        action_date="20181019",
        award_modification_amendme="0",
        is_active=True,
    )
    # Earliest record inactive, newer record has valid entry, inactive date matching active doesn't mess it up
    pub_fabs_5 = PublishedFABSFactory(
        awarding_office_code="abc",
        unique_award_key="4321_cba",
        action_date="20181018",
        award_modification_amendme="0",
        is_active=False,
    )
    pub_fabs_6 = PublishedFABSFactory(
        awarding_office_code="abc",
        unique_award_key="4321_cba",
        action_date="20181019",
        award_modification_amendme="1",
        is_active=False,
    )
    pub_fabs_7 = PublishedFABSFactory(
        awarding_office_code="12345a",
        unique_award_key="4321_cba",
        action_date="20181019",
        award_modification_amendme="1",
        is_active=True,
    )

    # New entry for base award with no office code
    fabs_1 = FABSFactory(
        awarding_office_code="",
        unique_award_key="zyxwv_123",
        action_date="20181020",
        award_modification_amendme="2",
        correction_delete_indicatr=None,
    )
    # New entry for base award with invalid code but entry has a awarding office code
    fabs_2 = FABSFactory(
        awarding_office_code="abd",
        unique_award_key="abcd_123",
        action_date="20181020",
        award_modification_amendme="1",
        correction_delete_indicatr=None,
    )
    # New entry for valid awarding office
    fabs_3 = FABSFactory(
        awarding_office_code=None,
        unique_award_key="1234_abc",
        action_date="20181020",
        award_modification_amendme="1",
        correction_delete_indicatr=None,
    )
    # Correction to base record (ignore)
    fabs_4 = FABSFactory(
        awarding_office_code="",
        unique_award_key="abcd_123",
        action_date="20181019",
        award_modification_amendme="0",
        correction_delete_indicatr="C",
    )
    # New entry for earliest inactive
    fabs_5 = FABSFactory(
        awarding_office_code="",
        unique_award_key="4321_cba",
        action_date="20181020",
        award_modification_amendme="2",
        correction_delete_indicatr=None,
    )
    errors = number_of_errors(
        _FILE,
        database,
        models=[
            office,
            pub_fabs_1,
            pub_fabs_2,
            pub_fabs_3,
            pub_fabs_4,
            pub_fabs_5,
            pub_fabs_6,
            pub_fabs_7,
            fabs_1,
            fabs_2,
            fabs_3,
            fabs_4,
            fabs_5,
        ],
    )
    assert errors == 0


def test_failure_invalid_offices(database):
    """Test fail that empty awarding office codes aren't matching invalid office codes from the base record."""

    office_1 = OfficeFactory(
        office_code="12345a",
        financial_assistance_awards_office=True,
        effective_start_date="01/01/2018",
        effective_end_date=None,
    )
    office_2 = OfficeFactory(
        office_code="abcd",
        financial_assistance_awards_office=False,
        effective_start_date="01/01/2018",
        effective_end_date=None,
    )
    # Invalid code in record
    pub_fabs_1 = PublishedFABSFactory(
        awarding_office_code="abc",
        unique_award_key="zyxwv_123",
        action_date="20181018",
        award_modification_amendme="0",
        is_active=True,
    )
    # Earliest record inactive, newer record has invalid entry
    pub_fabs_2 = PublishedFABSFactory(
        awarding_office_code="12345a",
        unique_award_key="4321_cba",
        action_date="20181018",
        award_modification_amendme="0",
        is_active=False,
    )
    pub_fabs_3 = PublishedFABSFactory(
        awarding_office_code="abc",
        unique_award_key="4321_cba",
        action_date="20181019",
        award_modification_amendme="1",
        is_active=True,
    )
    # Has a valid code but it's not an awarding code
    pub_fabs_4 = PublishedFABSFactory(
        awarding_office_code="abcd",
        unique_award_key="123_abc",
        action_date="20181018",
        award_modification_amendme="0",
        is_active=True,
    )
    # award_modification_amendme number is null
    pub_fabs_5 = PublishedFABSFactory(
        awarding_office_code="abc",
        unique_award_key="zyxwv_1234",
        action_date="20181018",
        award_modification_amendme=None,
        is_active=True,
    )

    # Entry for invalid code in base record
    fabs_1 = FABSFactory(
        awarding_office_code="",
        unique_award_key="zyxwv_123",
        action_date="20181020",
        award_modification_amendme="2",
        correction_delete_indicatr=None,
    )
    # Entry with award_modification_amendme null
    fabs_2 = FABSFactory(
        awarding_office_code="",
        unique_award_key="zyxwv_123",
        action_date="20181020",
        award_modification_amendme=None,
        correction_delete_indicatr=None,
    )
    # New entry for earliest inactive
    fabs_3 = FABSFactory(
        awarding_office_code="",
        unique_award_key="4321_cba",
        action_date="20181020",
        award_modification_amendme="2",
        correction_delete_indicatr=None,
    )
    # New entry for has valid non-awarding code
    fabs_4 = FABSFactory(
        awarding_office_code="",
        unique_award_key="123_abc",
        action_date="20181020",
        award_modification_amendme="2",
        correction_delete_indicatr=None,
    )
    # Entry for award_modification_amendme null in base record
    fabs_5 = FABSFactory(
        awarding_office_code="",
        unique_award_key="zyxwv_1234",
        action_date="20181020",
        award_modification_amendme="2",
        correction_delete_indicatr=None,
    )
    errors = number_of_errors(
        _FILE,
        database,
        models=[
            office_1,
            office_2,
            pub_fabs_1,
            pub_fabs_2,
            pub_fabs_3,
            pub_fabs_4,
            pub_fabs_5,
            fabs_1,
            fabs_2,
            fabs_3,
            fabs_4,
            fabs_5,
        ],
    )
    assert errors == 5


def test_failure_invalid_dates(database):
    """Test fail bad dates."""

    office_1 = OfficeFactory(
        office_code="12345a",
        financial_assistance_awards_office=True,
        effective_start_date="01/01/2018",
        effective_end_date="02/01/2018",
    )
    # Action date too early
    pub_fabs_1 = PublishedFABSFactory(
        awarding_office_code="12345a",
        unique_award_key="zyx_123",
        action_date="20171018",
        award_modification_amendme="0",
        is_active=True,
    )
    # Action date too late
    pub_fabs_2 = PublishedFABSFactory(
        awarding_office_code="12345a",
        unique_award_key="4321_cba",
        action_date="20181018",
        award_modification_amendme="0",
        is_active=True,
    )

    # Entry for action date too early
    fabs_1 = FABSFactory(
        awarding_office_code="",
        unique_award_key="zyx_123",
        action_date="20201020",
        award_modification_amendme="2",
        correction_delete_indicatr=None,
    )
    # Entry for action date too late
    fabs_2 = FABSFactory(
        awarding_office_code="",
        unique_award_key="4321_cba",
        action_date="20201020",
        award_modification_amendme=None,
        correction_delete_indicatr=None,
    )
    errors = number_of_errors(_FILE, database, models=[office_1, pub_fabs_1, pub_fabs_2, fabs_1, fabs_2])
    assert errors == 2
