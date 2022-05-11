from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import SubTierAgency, CGAC
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs21_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'funding_sub_tier_agency_co', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test FundingSubTierAgencyCode is an optional field, but when provided must be a valid 4-digit sub-tier agency
        code.
    """

    subcode = SubTierAgency(sub_tier_agency_code='A000', cgac_id='1')
    cgac = CGAC(cgac_id='1', cgac_code='001', agency_name='test')
    fabs = FABSFactory(funding_sub_tier_agency_co='A000', correction_delete_indicatr='')
    fabs_2 = FABSFactory(funding_sub_tier_agency_co='a000', correction_delete_indicatr=None)
    fabs_3 = FABSFactory(funding_sub_tier_agency_co=None, correction_delete_indicatr='c')
    fabs_4 = FABSFactory(funding_sub_tier_agency_co='', correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    fabs_5 = FABSFactory(funding_sub_tier_agency_co='bad', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5, subcode, cgac])
    assert errors == 0


def test_failure(database):
    """ Test failure FundingSubTierAgencyCode is an optional field, but when provided must be a valid 4-digit sub-tier
        agency code.
    """

    fabs = FABSFactory(funding_sub_tier_agency_co='bad', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[fabs])
    assert errors == 1
