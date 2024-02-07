from datetime import date
from decimal import Decimal

from dataactcore.scripts.pipeline.get_fsrs_updates import get_award_updates

from tests.unit.dataactcore.factories.staging import PublishedFABSFactory


def test_ignore_earlier_date(database):
    sess = database.session
    pf1 = PublishedFABSFactory(modified_at=date(2024, 2, 1), record_type=2, assistance_type='04', fain='tooearly')
    pf2 = PublishedFABSFactory(modified_at=date(2024, 2, 4), record_type=2, assistance_type='04', fain='justright')
    sess.add_all([pf1, pf2])
    sess.commit()

    results = get_award_updates('02/03/2024')
    result_list = results.all()

    assert len(result_list) == 1
    assert result_list[0].federal_award_id == 'justright'


def test_ignore_assistance_type(database):
    sess = database.session
    pf1 = PublishedFABSFactory(modified_at=date(2024, 2, 4), record_type=2, assistance_type='04', fain='rightassist')
    pf2 = PublishedFABSFactory(modified_at=date(2024, 2, 4), record_type=2, assistance_type='01', fain='badassist')
    sess.add_all([pf1, pf2])
    sess.commit()

    results = get_award_updates('02/03/2024')
    result_list = results.all()

    assert len(result_list) == 1
    assert result_list[0].federal_award_id == 'rightassist'


def test_ignore_record_type(database):
    sess = database.session
    pf1 = PublishedFABSFactory(modified_at=date(2024, 2, 4), record_type=2, assistance_type='04', fain='rightrecord')
    pf2 = PublishedFABSFactory(modified_at=date(2024, 2, 4), record_type=1, assistance_type='04', fain='badrecord')
    sess.add_all([pf1, pf2])
    sess.commit()

    results = get_award_updates('02/03/2024')
    result_list = results.all()

    assert len(result_list) == 1
    assert result_list[0].federal_award_id == 'rightrecord'


def test_status(database):
    sess = database.session
    pf1 = PublishedFABSFactory(modified_at=date(2024, 2, 4), record_type=2, assistance_type='04', fain='activerecord',
                               is_active=False)
    pf2 = PublishedFABSFactory(modified_at=date(2024, 1, 4), record_type=2, assistance_type='04', fain='activerecord',
                               is_active=True)
    pf3 = PublishedFABSFactory(modified_at=date(2024, 2, 4), record_type=2, assistance_type='04', fain='inactiverecord',
                               is_active=False)
    pf4 = PublishedFABSFactory(modified_at=date(2024, 2, 1), record_type=2, assistance_type='04', fain='inactiverecord',
                               is_active=False)
    sess.add_all([pf1, pf2, pf3, pf4])
    sess.commit()

    results = get_award_updates('02/03/2024')
    result_list = results.all()

    assert len(result_list) == 2
    for result in result_list:
        if result.federal_award_id == 'activerecord':
            assert result.status == 'active'
        else:
            assert result.status == 'inactive'


def test_grouping(database):
    sess = database.session
    pf1 = PublishedFABSFactory(modified_at=date(2024, 2, 1), record_type=2, assistance_type='04', fain='record',
                               period_of_performance_star=date(2000, 3, 22),
                               period_of_performance_curr=date(2025, 9, 5),
                               is_active=True, federal_action_obligation=25.5)
    pf2 = PublishedFABSFactory(modified_at=date(2024, 2, 1), record_type=2, assistance_type='04', fain='record',
                               period_of_performance_star=date(2003, 3, 22),
                               period_of_performance_curr=date(2035, 8, 5),
                               is_active=True, federal_action_obligation=30.25)
    pf3 = PublishedFABSFactory(modified_at=date(2024, 2, 4), record_type=2, assistance_type='04', fain='record',
                               period_of_performance_star=date(2023, 3, 22),
                               period_of_performance_curr=date(2025, 9, 5),
                               is_active=True, federal_action_obligation=4.3)
    pf4 = PublishedFABSFactory(modified_at=date(2024, 1, 4), record_type=2, assistance_type='04', fain='record',
                               period_of_performance_star=date(2023, 3, 22),
                               period_of_performance_curr=date(2025, 9, 5),
                               is_active=False, federal_action_obligation=100)
    sess.add_all([pf1, pf2, pf3, pf4])
    sess.commit()

    results = get_award_updates('02/03/2024')
    result_list = results.all()

    assert len(result_list) == 1
    assert result_list[0].starting_date == '2000-03-22'
    assert result_list[0].ending_date == '2035-08-05'
    assert result_list[0].total_fed_funding_amount == Decimal('60.05')
