from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactcore.factories.domain import OfficeFactory
from dataactcore.models.domainModels import SubTierAgency, CGAC, FREC
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs23_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'awarding_sub_tier_agency_c', 'awarding_office_code'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ If both are submitted, AwardingSubTierAgencyCode and AwardingOfficeCode must belong to the same
        AwardingAgencyCode (per the Federal Hierarchy). Ignored if one or both are missing. """

    cgac = CGAC(cgac_id=1, cgac_code='001', agency_name='test')
    frec = FREC(frec_id=1, cgac_id=1, frec_code='0001', agency_name='test2')
    # sub tier codes are different on these offices to prove that we don't care if the office is under that sub tier
    # as long as the top tier codes match
    office_1 = OfficeFactory(office_code='12345a', sub_tier_code='abcd', agency_code=cgac.cgac_code)
    office_2 = OfficeFactory(office_code='123457', sub_tier_code='efgh', agency_code=frec.frec_code)
    agency_1 = SubTierAgency(sub_tier_agency_code='a000', cgac_id=1, frec_id=1, is_frec=False)
    agency_2 = SubTierAgency(sub_tier_agency_code='0001', cgac_id=1, frec_id=1, is_frec=True)

    # Same agency for cgac
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c=agency_1.sub_tier_agency_code,
                                                          awarding_office_code=office_1.office_code)
    # Same agency for cgac (uppercased)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c=agency_1.sub_tier_agency_code.
                                                          upper(),
                                                          awarding_office_code=office_1.office_code.upper())
    # Same agency for frec
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c=agency_2.sub_tier_agency_code,
                                                          awarding_office_code=office_2.office_code)
    # Missing sub tier code
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='',
                                                          awarding_office_code=office_2.office_code)
    # Missing office code
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c=agency_1.sub_tier_agency_code,
                                                          awarding_office_code=None)

    errors = number_of_errors(_FILE, database, models=[cgac, frec, office_1, office_2, agency_1, agency_2, det_award_1,
                                                       det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test failure if both are submitted, AwardingSubTierAgencyCode and AwardingOfficeCode must belong to the same
        AwardingAgencyCode (per the Federal Hierarchy). """

    cgac = CGAC(cgac_id=1, cgac_code='001', agency_name='test')
    frec = FREC(frec_id=1, cgac_id=1, frec_code='0001', agency_name='test2')
    office = OfficeFactory(office_code='123456', sub_tier_code='abcd', agency_code=cgac.cgac_code)
    agency = SubTierAgency(sub_tier_agency_code='0000', frec_id=1, cgac_id=1, is_frec=True)

    # Sub tier is FREC, office is based on CGAC, the numbers are different
    det_award = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c=agency.sub_tier_agency_code,
                                                        awarding_office_code=office.office_code)

    errors = number_of_errors(_FILE, database, models=[det_award, cgac, frec, office, agency])
    assert errors == 1
