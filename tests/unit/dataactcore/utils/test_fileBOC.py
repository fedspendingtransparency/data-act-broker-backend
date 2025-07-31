import pandas as pd

from dataactcore.models.lookups import PUBLISH_STATUS_DICT
from dataactcore.utils.fileBOC import query_data
from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.staging import PublishedObjectClassProgramActivityFactory
from tests.unit.dataactcore.factories.job import PublishStatusFactory, SubmissionFactory


def test_fileboc_query_data(database):
    session = database.session
    ps = PublishStatusFactory(
        publish_status_id=PUBLISH_STATUS_DICT["updated"],
    )
    submission = SubmissionFactory(
        submission_id=1,
        reporting_fiscal_year=2025,
        reporting_fiscal_period=6,
        publish_status_id=ps.publish_status_id,
        cgac_code="123",
    )
    pocpa_group_props = {
        "submission_id": submission.submission_id,
        "display_tas": "test_tas",
        "allocation_transfer_agency": "456",
        "agency_identifier": "agency_id",
        "beginning_period_of_availa": "6",
        "ending_period_of_availabil": "6",
        "availability_type_code": "A",
        "main_account_code": "123",
        "sub_account_code": "456",
        "disaster_emergency_fund_code": "123",
        "budget_object_class": "123",
        "reimburseable_flag": "1",
        "begin_end": "123",
        "ussgl_number": "ussgl480100_undelivered_or_cpe",
        "object_class": "1234",
        "by_direct_reimbursable_fun": "abc",
        "prior_year_adjustment": "x",
        "ussgl480100_undelivered_or_cpe": 100,
        "ussgl480100_undelivered_or_fyb": 100,
        "ussgl480110_rein_undel_ord_cpe": 100,
        "ussgl480200_undelivered_or_cpe": 100,
        "ussgl480200_undelivered_or_fyb": 100,
        "ussgl483100_undelivered_or_cpe": 100,
        "ussgl483200_undelivered_or_cpe": 100,
        "ussgl487100_downward_adjus_cpe": 100,
        "ussgl487200_downward_adjus_cpe": 100,
        "ussgl488100_upward_adjustm_cpe": 100,
        "ussgl488200_upward_adjustm_cpe": 100,
        "ussgl490100_delivered_orde_cpe": 100,
        "ussgl490100_delivered_orde_fyb": 100,
        "ussgl490110_rein_deliv_ord_cpe": 100,
        "ussgl490200_delivered_orde_cpe": 100,
        "ussgl490800_authority_outl_cpe": 100,
        "ussgl490800_authority_outl_fyb": 100,
        "ussgl493100_delivered_orde_cpe": 100,
        "ussgl497100_downward_adjus_cpe": 100,
        "ussgl497200_downward_adjus_cpe": 100,
        "ussgl498100_upward_adjustm_cpe": 100,
        "ussgl498200_upward_adjustm_cpe": 100,
    }
    pocpa1 = PublishedObjectClassProgramActivityFactory(
        row_number=1,
        **pocpa_group_props,
    )
    pocpa2 = PublishedObjectClassProgramActivityFactory(
        row_number=2,
        **pocpa_group_props,
    )
    tas_lookup = TASFactory(
        display_tas="test_tas",
    )
    session.add_all([ps, submission, pocpa1, pocpa2, tas_lookup])
    session.commit()
    result = query_data(session, agency_code="123", period=6, year=2025)
    # Assert that the dolloar amounts all add up to 200
    assert (pd.DataFrame(result).dollar_amount_broker == 200).all()
