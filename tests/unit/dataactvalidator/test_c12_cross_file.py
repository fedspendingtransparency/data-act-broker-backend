from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardProcurementFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c12_cross_file'


def test_column_headers(database):
    expected_subset = {'source_row_number', 'source_value_piid', 'source_value_parent_award_id',
                       'source_value_federal_action_obligation', 'difference', 'uniqueid_PIID',
                       'uniqueid_ParentAwardId'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Unique PIID, ParentAwardId from file D1 exists in file C during the same reporting period, except D1 records
        with zero FederalActionObligation.
    """

    ap = AwardProcurementFactory(piid='some_pIId', parent_award_id='some_PArent_award_id', federal_action_obligation=1)
    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_parent_AWard_id', transaction_obligated_amou=1)

    assert number_of_errors(_FILE, database, models=[ap, af]) == 0

    # Rule shouldn't be checked if federal_action_obligation is null
    ap = AwardProcurementFactory(piid='some_piid', parent_award_id='some_PArent_award_id',
                                 federal_action_obligation=None)

    assert number_of_errors(_FILE, database, models=[ap]) == 0

    # Checks null = null
    ap = AwardProcurementFactory(piid='some_piid', parent_award_id=None, federal_action_obligation=1)
    af = AwardFinancialFactory(piid='some_pIId', parent_award_id=None, transaction_obligated_amou=0)

    assert number_of_errors(_FILE, database, models=[ap, af]) == 0


def test_failure(database):
    """ Unique PIID, ParentAwardId from file D1 doesn't exist in file C during the same reporting period,
        except D1 records with zero FederalActionObligation.
    """

    ap = AwardProcurementFactory(piid='some_pIId', parent_award_id='some_parent_AWArd_id', federal_action_obligation=1)
    af = AwardFinancialFactory(piid='some_other_piid', parent_award_id='some_pARent_award_id',
                               transaction_obligated_amou=1)

    assert number_of_errors(_FILE, database, models=[af, ap]) == 1

    ap = AwardProcurementFactory(piid='some_piid', parent_award_id='some_Parent_award_id', federal_action_obligation=1)
    af = AwardFinancialFactory(piid='some_pIId', parent_award_id='some_other_parenT_award_id',
                               transaction_obligated_amou=1)

    assert number_of_errors(_FILE, database, models=[af, ap]) == 1

    # Check to see that NULL TOA causes an error
    ap = AwardProcurementFactory(piid='some_pIId', parent_award_id='some_PArent_award_id', federal_action_obligation=1)
    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_parent_AWard_id',
                               transaction_obligated_amou=None)

    assert number_of_errors(_FILE, database, models=[ap, af]) == 1
