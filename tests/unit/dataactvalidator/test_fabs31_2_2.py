from tests.unit.dataactcore.factories.staging import FABSFactory, PublishedFABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs31_2_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "assistance_type",
        "action_date",
        "uei",
        "business_types",
        "record_type",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test success for AwardeeOrRecipientUEI is required where ActionDate is after October 1, 2010, unless the record
    is an aggregate or PII-redacted non-aggregate record (RecordType = 1 or 3) or the recipient is an individual
    (BusinessTypes includes 'P'). For AssistanceType 06, 07, 08, 09, 10, or 11, if the base award (the earliest
    record with the same unique award key) has an ActionDate prior to October 1, 2022, this will produce a warning
    rather than a fatal error.
    """
    # Note: for FABS 31.2.2, we're setting assistance types to 06, 07, 08, 09, 10, or 11 and having the base
    #       actiondate be less than October 1, 2032. This rule will not trigger if those *do* apply.
    #       FABS 31.2.1 *will not* trigger when these apply.

    pub_fabs_1 = PublishedFABSFactory(unique_award_key="before_key", action_date="20091001", is_active=True)
    pub_fabs_2 = PublishedFABSFactory(unique_award_key="after_key", action_date="20330404", is_active=True)
    pub_fabs_3 = PublishedFABSFactory(unique_award_key="inactive_key", action_date="20091001", is_active=False)
    models = [pub_fabs_1, pub_fabs_2, pub_fabs_3]

    # new records that may or may not be related to older awards
    fabs_1 = FABSFactory(
        record_type=2,
        business_types="AbC",
        uei="test",
        action_date="10/02/2010",
        assistance_type="06",
        correction_delete_indicatr="",
        unique_award_key="before_key",
    )
    fabs_2 = FABSFactory(
        record_type=5,
        business_types="aBc",
        uei="test",
        action_date="10/02/2010",
        assistance_type="07",
        correction_delete_indicatr="c",
        unique_award_key="before_key",
    )
    # Ignored for dates before Oct 1 2010
    fabs_3 = FABSFactory(
        record_type=4,
        business_types="AbC",
        uei="",
        action_date="09/02/2010",
        assistance_type="08",
        correction_delete_indicatr="C",
        unique_award_key="before_key",
    )
    # Ignored for record type 1/3
    fabs_4 = FABSFactory(
        record_type=1,
        business_types="aBc",
        uei=None,
        action_date="10/02/2010",
        assistance_type="09",
        correction_delete_indicatr=None,
        unique_award_key="before_key",
    )
    # Ignored for other assistance types
    fabs_5 = FABSFactory(
        record_type=2,
        business_types="AbC",
        uei=None,
        action_date="10/02/2010",
        assistance_type="02",
        correction_delete_indicatr="",
        unique_award_key="before_key",
    )
    # Ignored when business types include P
    fabs_6 = FABSFactory(
        record_type=5,
        business_types="aPc",
        uei=None,
        action_date="10/02/2010",
        assistance_type="11",
        correction_delete_indicatr="",
        unique_award_key="before_key",
    )
    # Ignore correction delete indicator of D
    fabs_7 = FABSFactory(
        record_type=2,
        business_types="AbC",
        uei=None,
        action_date="10/02/2010",
        assistance_type="06",
        correction_delete_indicatr="d",
        unique_award_key="before_key",
    )
    # Ensuring that this rule gets ignored when the base actiondate case doesn't apply
    fabs_8 = FABSFactory(
        record_type=2,
        business_types="AbC",
        uei="",
        action_date="10/02/2010",
        assistance_type="06",
        correction_delete_indicatr="",
        unique_award_key="after_key",
    )
    fabs_9 = FABSFactory(
        record_type=2,
        business_types="AbC",
        uei="",
        action_date="04/05/2033",
        assistance_type="06",
        correction_delete_indicatr="",
        unique_award_key="inactive_key",
    )
    fabs_10 = FABSFactory(
        record_type=2,
        business_types="AbC",
        uei="",
        action_date="04/05/2033",
        assistance_type="06",
        correction_delete_indicatr="",
        unique_award_key="new_key",
    )
    models += [fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8, fabs_9, fabs_10]

    errors = number_of_errors(_FILE, database, models=models)
    assert errors == 0


def test_failure(database):
    """Test failure for AwardeeOrRecipientUEI is required where ActionDate is after October 1, 2010, unless the record
    is an aggregate or PII-redacted non-aggregate record (RecordType = 1 or 3) or the recipient is an individual
    (BusinessTypes includes 'P'). For AssistanceType 06, 07, 08, 09, 10, or 11, if the base award (the earliest
    record with the same unique award key) has an ActionDate prior to October 1, 2032, this will produce a warning
    rather than a fatal error.
    """
    # Note: for FABS 31.2.2, we're setting assistance types to 06, 07, 08, 09, 10, or 11 and having the base
    #       actiondate be less than October 1, 2032. This rule will not trigger if those *do* apply.
    #       FABS 31.2.1 *will not* trigger when these apply.

    pub_fabs_1 = PublishedFABSFactory(unique_award_key="before_key", action_date="20091001", is_active=True)
    pub_fabs_2 = PublishedFABSFactory(unique_award_key="after_key", action_date="20230404", is_active=True)
    pub_fabs_3 = PublishedFABSFactory(unique_award_key="inactive_key", action_date="20091001", is_active=False)
    models = [pub_fabs_1, pub_fabs_2, pub_fabs_3]

    fabs_1 = FABSFactory(
        record_type=2,
        business_types="AbC",
        uei=None,
        action_date="10/02/2010",
        assistance_type="06",
        correction_delete_indicatr="",
        unique_award_key="before_key",
    )
    fabs_2 = FABSFactory(
        record_type=5,
        business_types="aBc",
        uei=None,
        action_date="10/02/2010",
        assistance_type="07",
        correction_delete_indicatr="C",
        unique_award_key="before_key",
    )
    fabs_3 = FABSFactory(
        record_type=4,
        business_types="AbC",
        uei="",
        action_date="10/02/2010",
        assistance_type="08",
        correction_delete_indicatr="c",
        unique_award_key="before_key",
    )
    fabs_4 = FABSFactory(
        record_type=5,
        business_types="aBc",
        uei="",
        action_date="10/02/2010",
        assistance_type="09",
        correction_delete_indicatr=None,
        unique_award_key="before_key",
    )
    fabs_5 = FABSFactory(
        record_type=2,
        business_types="AbC",
        uei="",
        action_date="10/02/2010",
        assistance_type="06",
        correction_delete_indicatr="",
        unique_award_key="inactive_key",
    )
    fabs_6 = FABSFactory(
        record_type=2,
        business_types="AbC",
        uei="",
        action_date="10/02/2010",
        assistance_type="06",
        correction_delete_indicatr="",
        unique_award_key="new_key",
    )
    models += [fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6]

    errors = number_of_errors(_FILE, database, models=models)
    assert errors == 6
