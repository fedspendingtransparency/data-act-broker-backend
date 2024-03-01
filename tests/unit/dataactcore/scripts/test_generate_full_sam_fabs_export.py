from datetime import date, datetime, timedelta

from dataactcore.scripts.ad_hoc.generate_full_sam_fabs_export import get_awards

from tests.unit.dataactcore.factories.staging import PublishedFABSFactory


def test_ignore_inactive(database):
    sess = database.session
    pf1 = PublishedFABSFactory(updated_at=date(2024, 2, 1), is_active=True, fain='activerecord')
    pf2 = PublishedFABSFactory(updated_at=date(2024, 2, 4), is_active=False, fain='inactiverecord')
    sess.add_all([pf1, pf2])
    sess.commit()

    results = get_awards()
    result_list = results.all()

    assert len(result_list) == 1
    assert result_list[0].federal_award_id == 'activerecord'


def test_cumulative_sum(database):
    # Also testing that the right values are used to roll up the sum
    sess = database.session
    pf1 = PublishedFABSFactory(action_date=date(2000, 3, 22), is_active=True, fain='seconddate', assistance_type='07',
                               original_loan_subsidy_cost=52, federal_action_obligation=20, unique_award_key='abc')
    pf2 = PublishedFABSFactory(action_date=date(2000, 3, 22), is_active=True, fain='seconddate', assistance_type='07',
                               original_loan_subsidy_cost=8, federal_action_obligation=20, unique_award_key='abc')
    pf3 = PublishedFABSFactory(action_date=date(2001, 1, 1), is_active=True, fain='thirddate', assistance_type='02',
                               original_loan_subsidy_cost=9.5, federal_action_obligation=11, unique_award_key='abc')
    pf4 = PublishedFABSFactory(action_date=date(2000, 1, 30), is_active=True, fain='firstdate', assistance_type='06',
                               original_loan_subsidy_cost=13, federal_action_obligation=5, unique_award_key='abc')
    pf5 = PublishedFABSFactory(action_date=date(2000, 1, 30), is_active=True, fain='otherkey', assistance_type='08',
                               original_loan_subsidy_cost=5, federal_action_obligation=9, unique_award_key='def')
    sess.add_all([pf1, pf2, pf3, pf4, pf5])
    sess.commit()

    results = get_awards()
    result_list = results.all()

    assert len(result_list) == 5
    for result in result_list:
        if result.federal_award_id == 'firstdate':
            assert result.total_fed_funding_amount == 5
        elif result.federal_award_id == 'seconddate':
            # If there are 2 in the same date they should get rolled up together
            assert result.total_fed_funding_amount == 65
        elif result.federal_award_id == 'thirddate':
            assert result.total_fed_funding_amount == 76
        else:
            assert result.total_fed_funding_amount == 5
