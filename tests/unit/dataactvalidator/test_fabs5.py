from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs5"


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_type", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Tests success for when AssistanceType field is required and must be one of the allowed values:
    '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', 'F001', 'F002', 'F003', 'F004', 'F005', 'F006', 'F007', 'F008', 'F009', 'F010'
    """
    fabs_1 = FABSFactory(assistance_type="02", correction_delete_indicatr="")
    fabs_2 = FABSFactory(assistance_type="03", correction_delete_indicatr=None)
    fabs_3 = FABSFactory(assistance_type="04", correction_delete_indicatr="C")
    fabs_4 = FABSFactory(assistance_type="05", correction_delete_indicatr="c")
    fabs_5 = FABSFactory(assistance_type="06", correction_delete_indicatr="")
    fabs_6 = FABSFactory(assistance_type="07", correction_delete_indicatr="")
    fabs_7 = FABSFactory(assistance_type="08", correction_delete_indicatr="")
    fabs_8 = FABSFactory(assistance_type="09", correction_delete_indicatr="")
    fabs_9 = FABSFactory(assistance_type="10", correction_delete_indicatr="")
    fabs_10 = FABSFactory(assistance_type="11", correction_delete_indicatr="")
    fabs_11 = FABSFactory(assistance_type="F001", correction_delete_indicatr="")
    fabs_12 = FABSFactory(assistance_type="F002", correction_delete_indicatr="")
    fabs_13 = FABSFactory(assistance_type="F003", correction_delete_indicatr="")
    fabs_14 = FABSFactory(assistance_type="F004", correction_delete_indicatr="")
    fabs_15 = FABSFactory(assistance_type="F005", correction_delete_indicatr="")
    fabs_16 = FABSFactory(assistance_type="F006", correction_delete_indicatr="")
    fabs_17 = FABSFactory(assistance_type="F007", correction_delete_indicatr="")
    fabs_18 = FABSFactory(assistance_type="F008", correction_delete_indicatr="")
    fabs_19 = FABSFactory(assistance_type="F009", correction_delete_indicatr="")
    fabs_20 = FABSFactory(assistance_type="F010", correction_delete_indicatr="")

    # Ignore correction delete indicator of D
    fabs_21 = FABSFactory(assistance_type="Thing", correction_delete_indicatr="d")

    errors = number_of_errors(
        _FILE,
        database,
        models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8, fabs_9, fabs_10, fabs_11, fabs_12, fabs_13, fabs_14, fabs_15, fabs_16, fabs_17, fabs_18, fabs_19, fabs_20, fabs_21],
    )
    assert errors == 0


def test_failure(database):
    """Tests failure for when AssistanceType field is required and must be one of the allowed values:
    '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', 'F001', 'F002', 'F003', 'F004', 'F005', 'F006', 'F007', 'F008', 'F009', 'F010'
    """
    fabs_1 = FABSFactory(assistance_type="", correction_delete_indicatr="")
    fabs_2 = FABSFactory(assistance_type=None, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(assistance_type="random", correction_delete_indicatr="c")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3])
    assert errors == 3
