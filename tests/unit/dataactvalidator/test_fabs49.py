from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory, SubTierAgencyFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs49"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "awarding_sub_tier_agency_c",
        "derived_awarding_agency_code",
        "expected_value_Derived AwardingAgencyCode",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success_cgac(database):
    """Test A submitted Financial Assistance award must have a (derived) AwardingAgencyCode that is consistent with
    the toptier component of the agency selected at the outset of the FABS submission. This comparison only takes
    place at the TopTier level, not the SubTier level. CGAC tests
    """
    cgac = CGACFactory(cgac_code="123")
    cgac_frec = FRECFactory(cgac=cgac)
    cgac_subtier = SubTierAgencyFactory(sub_tier_agency_code="1234", cgac=cgac, frec=cgac_frec, is_frec=False)
    cgac_submission = SubmissionFactory(cgac_code=cgac.cgac_code, frec_code=None)

    fabs_1 = FABSFactory(awarding_sub_tier_agency_c="1234", correction_delete_indicatr="C")

    # Ignored if sub tier agency code doesn't exist because that's handled elsewhere
    fabs_2 = FABSFactory(awarding_sub_tier_agency_c="9876", correction_delete_indicatr="C")

    errors = number_of_errors(
        _FILE, database, models=[fabs_1, fabs_2, cgac, cgac_frec, cgac_subtier], submission=cgac_submission
    )
    assert errors == 0


def test_success_frec(database):
    """Test A submitted Financial Assistance award must have a (derived) AwardingAgencyCode that is consistent with
    the toptier component of the agency selected at the outset of the FABS submission. This comparison only takes
    place at the TopTier level, not the SubTier level. FREC tests
    """
    frec_cgac = CGACFactory()
    frec = FRECFactory(frec_code="4567", cgac=frec_cgac)
    frec_subtier = SubTierAgencyFactory(sub_tier_agency_code="5678", cgac=frec_cgac, frec=frec, is_frec=True)
    frec_submission = SubmissionFactory(cgac_code=None, frec_code=frec.frec_code)

    fabs_1 = FABSFactory(awarding_sub_tier_agency_c="5678", correction_delete_indicatr=None)

    # Ignored if sub tier agency code doesn't exist because that's handled elsewhere
    fabs_2 = FABSFactory(awarding_sub_tier_agency_c="9876", correction_delete_indicatr=None)

    errors = number_of_errors(
        _FILE, database, models=[fabs_1, fabs_2, frec_subtier, frec_cgac, frec], submission=frec_submission
    )
    assert errors == 0


def test_failure_cgac(database):
    """Test failure A submitted Financial Assistance award must have a (derived) AwardingAgencyCode that is consistent
    with the toptier component of the agency selected at the outset of the FABS submission. This comparison only
    takes place at the TopTier level, not the SubTier level. CGAC tests
    """
    cgac = CGACFactory(cgac_code="123")
    cgac_frec = FRECFactory(cgac=cgac)
    cgac_subtier = SubTierAgencyFactory(sub_tier_agency_code="1234", cgac=cgac, frec=cgac_frec, is_frec=False)
    cgac_submission = SubmissionFactory(cgac_code="321", frec_code=None)

    fabs_1 = FABSFactory(awarding_sub_tier_agency_c="1234", correction_delete_indicatr=None)

    # Don't ignore correction delete indicator of D
    fabs_2 = FABSFactory(awarding_sub_tier_agency_c="1234", correction_delete_indicatr="d")

    errors = number_of_errors(
        _FILE, database, models=[fabs_1, fabs_2, cgac, cgac_frec, cgac_subtier], submission=cgac_submission
    )
    assert errors == 2


def test_failure_frec(database):
    """Test failure A submitted Financial Assistance award must have a (derived) AwardingAgencyCode that is consistent
    with the toptier component of the agency selected at the outset of the FABS submission. This comparison only
    takes place at the TopTier level, not the SubTier level. FREC tests
    """
    frec_cgac = CGACFactory()
    frec = FRECFactory(frec_code="4567", cgac=frec_cgac)
    frec_subtier = SubTierAgencyFactory(sub_tier_agency_code="5678", cgac=frec_cgac, frec=frec, is_frec=True)
    frec_submission = SubmissionFactory(cgac_code=None, frec_code="1234")

    fabs_1 = FABSFactory(awarding_sub_tier_agency_c="5678", correction_delete_indicatr=None)

    # Don't ignore correction delete indicator of D
    fabs_2 = FABSFactory(awarding_sub_tier_agency_c="5678", correction_delete_indicatr="d")

    errors = number_of_errors(
        _FILE, database, models=[fabs_1, fabs_2, frec_cgac, frec, frec_subtier], submission=frec_submission
    )
    assert errors == 2
