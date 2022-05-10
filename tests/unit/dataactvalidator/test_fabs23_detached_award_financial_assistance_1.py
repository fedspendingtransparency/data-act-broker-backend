from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import SubTierAgency, CGAC
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs23_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'awarding_sub_tier_agency_c', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ AwardingSubTierAgencyCode must be a valid 4-digit sub-tier agency code. Doesn't fail when code not provided. """

    agency = SubTierAgency(sub_tier_agency_code='a000', cgac_id='1')
    cgac = CGAC(cgac_id='1', cgac_code='001', agency_name='test')
    fabs = FABSFactory(awarding_sub_tier_agency_c=agency.sub_tier_agency_code.upper(), correction_delete_indicatr='')
    fabs_2 = FABSFactory(awarding_sub_tier_agency_c=None, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(awarding_sub_tier_agency_c='', correction_delete_indicatr='c')
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(awarding_sub_tier_agency_c='bad', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, agency, cgac])
    assert errors == 0


def test_failure(database):
    """ AwardingSubTierAgencyCode must be a valid 4-digit sub-tier agency code. """

    fabs = FABSFactory(awarding_sub_tier_agency_c='bad', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[fabs])
    assert errors == 1
