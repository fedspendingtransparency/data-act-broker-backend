from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory, TASFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a33_appropriations_3'


def test_column_headers(database):
    expected_subset = {'uniqueid_TAS', 'row_number', 'allocation_transfer_agency', 'agency_identifier',
                       'beginning_period_of_availa', 'ending_period_of_availabil', 'availability_type_code',
                       'main_account_code', 'sub_account_code', 'expected_value_ATA'}
    actual = set(query_columns(_FILE, database))
    assert actual == expected_subset


def test_success(database):
    """ Tests that TAS with no ATA or matching ATA pass. """
    sub = SubmissionFactory(cgac_code='abc')
    cgac = CGACFactory(cgac_code='abc')
    tas = TASFactory(financial_indicator2='f')

    # Matching ATA
    ap1 = AppropriationFactory(submission_id=sub.submission_id, allocation_transfer_agency='abc',
                               adjustments_to_unobligated_cpe=15)
    # Blank ATA
    ap2 = AppropriationFactory(submission_id=sub.submission_id, allocation_transfer_agency='',
                               adjustments_to_unobligated_cpe=15)
    ap3 = AppropriationFactory(submission_id=sub.submission_id, allocation_transfer_agency=None,
                               adjustments_to_unobligated_cpe=15)
    # Non-matching ATA with all monetary values 0 (with one None for good measure)
    ap4 = AppropriationFactory(submission_id=sub.submission_id, allocation_transfer_agency='Not a match',
                               adjustments_to_unobligated_cpe=0, budget_authority_appropria_cpe=0,
                               borrowing_authority_amount_cpe=0, contract_authority_amount_cpe=None,
                               spending_authority_from_of_cpe=0, other_budgetary_resources_cpe=0,
                               total_budgetary_resources_cpe=0, gross_outlay_amount_by_tas_cpe=0,
                               obligations_incurred_total_cpe=0, deobligations_recoveries_r_cpe=0,
                               unobligated_balance_cpe=0, status_of_budgetary_resour_cpe=0)
    # Non-matching ATA with financial_indicator2 of F
    ap5 = AppropriationFactory(submission_id=sub.submission_id, allocation_transfer_agency='Not a match',
                               tas_id=tas.account_num, adjustments_to_unobligated_cpe=15)

    assert number_of_errors(_FILE, database, submission=sub, models=[cgac, tas, ap1, ap2, ap3, ap4, ap5]) == 0

    cgac = CGACFactory(cgac_code='123')
    database.session.add(cgac)
    database.session.commit()

    frec = FRECFactory(frec_code='abcd', cgac_id=cgac.cgac_id)
    sub = SubmissionFactory(cgac_code=None, frec_code=frec.frec_code)

    # Matching ATA for a FREC
    ap = AppropriationFactory(submission_id=sub.submission_id, allocation_transfer_agency='123',
                              adjustments_to_unobligated_cpe=15)

    assert number_of_errors(_FILE, database, submission=sub, models=[frec, ap]) == 0


def test_failure(database):
    """ Tests that TAS with non-matching ATA don't pass. """
    sub = SubmissionFactory(cgac_code='abc')
    cgac = CGACFactory(cgac_code='abc')

    ap = AppropriationFactory(submission_id=sub.submission_id, allocation_transfer_agency='Not a match',
                              adjustments_to_unobligated_cpe=1)

    assert number_of_errors(_FILE, database, submission=sub, models=[cgac, ap]) == 1
