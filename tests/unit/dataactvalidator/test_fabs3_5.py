from tests.unit.dataactcore.factories.staging import FABSFactory, PublishedFABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs3_5"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "fain",
        "uri",
        "awarding_sub_tier_agency_c",
        "action_type",
        "record_type",
        "correction_delete_indicatr",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ActionType should be "A" for the initial transaction of a new, non-aggregate award (RecordType = 2 or 3) and
    "A" or "E" for a new aggregate award (RecordType = 1). An aggregate record transaction is considered the
    initial transaction of a new award if it contains a unique combination of URI + AwardingSubTierAgencyCode when
    compared to currently published FABS records of the same RecordType. A non-aggregate (RecordType = 2 or 3)
    transaction is considered the initial transaction of a new award if it contains a unique combination of FAIN +
    AwardingSubTierAgencyCode when compared to currently published non-aggregate FABS records (RecordType = 2 or 3)
    of the same RecordType.
    """
    fabs_1 = FABSFactory(unique_award_key="unique1", action_type="A", record_type=1, correction_delete_indicatr=None)
    fabs_2 = FABSFactory(unique_award_key="unique1", action_type="e", record_type=1, correction_delete_indicatr="")
    fabs_3 = FABSFactory(unique_award_key="unique2", action_type="a", record_type=3, correction_delete_indicatr="")

    # Ignore delete/correction record
    fabs_4 = FABSFactory(unique_award_key="unique1", action_type="C", record_type=3, correction_delete_indicatr="D")
    fabs_5 = FABSFactory(unique_award_key="unique1", action_type="C", record_type=3, correction_delete_indicatr="c")

    # This is an active award so it will be ignored
    fabs_6 = FABSFactory(unique_award_key="unique3", action_type="d", record_type=2, correction_delete_indicatr=None)
    fabs_7 = FABSFactory(unique_award_key="unique3", action_type="e", record_type=2, correction_delete_indicatr=None)

    pub_fabs_1 = PublishedFABSFactory(unique_award_key="unique2", is_active=False)
    pub_fabs_2 = PublishedFABSFactory(unique_award_key="unique3", is_active=True)

    errors = number_of_errors(
        _FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, pub_fabs_1, pub_fabs_2]
    )
    assert errors == 0


def test_failure(database):
    """Fail ActionType should be "A" for the initial transaction of a new, non-aggregate award (RecordType = 2 or 3)
    and "A" or "E" for a new aggregate award (RecordType = 1). An aggregate record transaction is considered the initial
    transaction of a new award if it contains a unique combination of URI + AwardingSubTierAgencyCode when compared to
    currently published FABS records of the same RecordType. A non-aggregate (RecordType = 2 or 3) transaction is
    considered the initial transaction of a new award if it contains a unique combination of FAIN +
    AwardingSubTierAgencyCode when compared to currently published non-aggregate FABS records (RecordType = 2 or 3) of
    the same RecordType.
    """

    fabs_1 = FABSFactory(unique_award_key="unique1", action_type="b", record_type=1, correction_delete_indicatr=None)

    # E is only valid for record type 1
    fabs_2 = FABSFactory(unique_award_key="unique2", action_type="E", record_type=2, correction_delete_indicatr="")

    pub_fabs_1 = PublishedFABSFactory(unique_award_key="unique2", is_active=False)

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, pub_fabs_1])
    assert errors == 2
