from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b13_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'row_number', 'deobligations_recov_by_pro_cpe', 'ussgl487100_downward_adjus_cpe',
                       'ussgl487200_downward_adjus_cpe', 'ussgl497100_downward_adjus_cpe',
                       'ussgl497200_downward_adjus_cpe'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE in File B = USSGL(4871+ 4872 + 4971 + 4972)
    in File B for the same reporting period """

    op = ObjectClassProgramActivityFactory(deobligations_recov_by_pro_cpe=None, ussgl487100_downward_adjus_cpe=None,
                                           ussgl487200_downward_adjus_cpe=None, ussgl497100_downward_adjus_cpe=None,
                                           ussgl497200_downward_adjus_cpe=None)

    assert number_of_errors(_FILE, database, models=[op]) == 0

    op = ObjectClassProgramActivityFactory(deobligations_recov_by_pro_cpe=4, ussgl487100_downward_adjus_cpe=1,
                                           ussgl487200_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
                                           ussgl497200_downward_adjus_cpe=1)

    assert number_of_errors(_FILE, database, models=[op]) == 0


def test_failure(database):
    """ DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE in File B != USSGL(4871+ 4872 + 4971 + 4972)
    in File B for the same reporting period """

    op = ObjectClassProgramActivityFactory(deobligations_recov_by_pro_cpe=1, ussgl487100_downward_adjus_cpe=None,
                                           ussgl487200_downward_adjus_cpe=None, ussgl497100_downward_adjus_cpe=None,
                                           ussgl497200_downward_adjus_cpe=None)

    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(deobligations_recov_by_pro_cpe=1, ussgl487100_downward_adjus_cpe=1,
                                           ussgl487200_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
                                           ussgl497200_downward_adjus_cpe=1)

    assert number_of_errors(_FILE, database, models=[op]) == 1