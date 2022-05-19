from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs2_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'fain', 'award_modification_amendme', 'uri', 'awarding_sub_tier_agency_c',
                       'cfda_number', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests that all combinations of FAIN, AwardModificationAmendmentNumber, URI, CFDA_Number, and
        AwardingSubTierAgencyCode in a FABS submission are unique
    """
    fabs_1 = FABSFactory(afa_generated_unique='abc_def_ghi', correction_delete_indicatr=None)
    fabs_2 = FABSFactory(afa_generated_unique='abc_def_ghij', correction_delete_indicatr='')
    fabs_3 = FABSFactory(afa_generated_unique='abcd_efg_hij', correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3])
    assert errors == 0


def test_failure(database):
    """ Tests that all combinations of FAIN, AwardModificationAmendmentNumber, URI, CFDA_Number, and
        AwardingSubTierAgencyCode in a FABS submission are not unique. Make sure casing is ignored.
    """

    fabs_1 = FABSFactory(afa_generated_unique='abc_def_ghi', correction_delete_indicatr=None)
    fabs_2 = FABSFactory(afa_generated_unique='aBC_def_ghi', correction_delete_indicatr='C')
    fabs_3 = FABSFactory(afa_generated_unique='abc_deF_ghi', correction_delete_indicatr='D')
    fabs_4 = FABSFactory(afa_generated_unique='abc_def_GHI', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 3
