from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b13_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'row_number', 'prior_year_adjustment', 'deobligations_recov_by_pro_cpe',
                       'ussgl487100_downward_adjus_cpe', 'ussgl487200_downward_adjus_cpe',
                       'ussgl497100_downward_adjus_cpe', 'ussgl497200_downward_adjus_cpe', 'difference', 'uniqueid_TAS',
                       'uniqueid_DisasterEmergencyFundCode', 'uniqueid_ProgramActivityCode', 'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE in File B = USSGL(4871+ 4872 + 4971 + 4972)
        in File B  for the same TAS/DEFC combination where PYA = "X".
    """

    op = ObjectClassProgramActivityFactory(deobligations_recov_by_pro_cpe=4, ussgl487100_downward_adjus_cpe=1,
                                           ussgl487200_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
                                           ussgl497200_downward_adjus_cpe=1, prior_year_adjustment='X')
    # Null check
    op2 = ObjectClassProgramActivityFactory(deobligations_recov_by_pro_cpe=None, ussgl487100_downward_adjus_cpe=None,
                                            ussgl487200_downward_adjus_cpe=None, ussgl497100_downward_adjus_cpe=None,
                                            ussgl497200_downward_adjus_cpe=None, prior_year_adjustment='x')
    # Different values, Different PYA
    op3 = ObjectClassProgramActivityFactory(deobligations_recov_by_pro_cpe=0, ussgl487100_downward_adjus_cpe=1,
                                            ussgl487200_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
                                            ussgl497200_downward_adjus_cpe=1, prior_year_adjustment='A')

    assert number_of_errors(_FILE, database, models=[op, op2, op3]) == 0


def test_failure(database):
    """ DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE in File B != USSGL(4871+ 4872 + 4971 + 4972)
        in File B for the same TAS/DEFC combination where PYA = "X".
    """

    op = ObjectClassProgramActivityFactory(deobligations_recov_by_pro_cpe=1, ussgl487100_downward_adjus_cpe=1,
                                           ussgl487200_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
                                           ussgl497200_downward_adjus_cpe=1, prior_year_adjustment='X')

    op2 = ObjectClassProgramActivityFactory(deobligations_recov_by_pro_cpe=1, ussgl487100_downward_adjus_cpe=None,
                                            ussgl487200_downward_adjus_cpe=None, ussgl497100_downward_adjus_cpe=None,
                                            ussgl497200_downward_adjus_cpe=None, prior_year_adjustment='x')

    assert number_of_errors(_FILE, database, models=[op, op2]) == 2
