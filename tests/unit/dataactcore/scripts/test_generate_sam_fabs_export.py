from datetime import date, datetime, timedelta

from dataactcore.scripts.pipeline.generate_sam_fabs_export import get_award_updates_query

from tests.unit.dataactcore.factories.staging import PublishedFABSFactory


def test_ignore_earlier_date(database):
    sess = database.session
    pf1 = PublishedFABSFactory(updated_at=date(2024, 2, 1), record_type=2, assistance_type='04', fain='tooearly')
    pf2 = PublishedFABSFactory(updated_at=date(2024, 2, 4), record_type=2, assistance_type='04', fain='justright')
    sess.add_all([pf1, pf2])
    sess.commit()

    results = sess.execute(get_award_updates_query('02/03/2024'))
    result_list = results.all()

    assert len(result_list) == 1
    assert result_list[0].federal_award_id == 'justright'


def test_status(database):
    sess = database.session
    pf1 = PublishedFABSFactory(updated_at=date(2024, 2, 4), afa_generated_unique='activerecord', is_active=False)
    pf2 = PublishedFABSFactory(updated_at=date(2024, 1, 4), afa_generated_unique='activerecord', is_active=True)
    pf3 = PublishedFABSFactory(updated_at=date(2024, 2, 4), afa_generated_unique='inactiverecord', is_active=False)
    pf4 = PublishedFABSFactory(updated_at=date(2024, 2, 1), afa_generated_unique='inactiverecord', is_active=False)
    sess.add_all([pf1, pf2, pf3, pf4])
    sess.commit()

    results = sess.execute(get_award_updates_query('02/03/2024'))
    result_list = results.all()

    assert len(result_list) == 2
    for result in result_list:
        if result.federal_award_id == 'activerecord':
            assert result.status == 'active'
        else:
            assert result.status == 'inactive'


def test_ignore_duplicates(database):
    sess = database.session
    pf1 = PublishedFABSFactory(updated_at=datetime.now() - timedelta(hours=1), afa_generated_unique='activerecord',
                               is_active=False, award_description='older entry')
    pf2 = PublishedFABSFactory(updated_at=datetime.now() + timedelta(minutes=1), afa_generated_unique='activerecord',
                               is_active=True, award_description='newer entry')
    sess.add_all([pf1, pf2])
    sess.commit()

    results = sess.execute(get_award_updates_query(datetime.now().strftime('%m/%d/%y')))
    result_list = results.all()

    assert len(result_list) == 1
    assert result_list[0].project_description == 'newer entry'


def test_grouping(database):
    sess = database.session
    pf1 = PublishedFABSFactory(updated_at=date(2024, 2, 1), unique_award_key='record', action_date=date(2000, 3, 22),
                               is_active=True, federal_action_obligation=25.5)
    pf2 = PublishedFABSFactory(updated_at=date(2024, 2, 1), unique_award_key='record', action_date=date(2003, 3, 22),
                               is_active=True, federal_action_obligation=30.25)
    pf3 = PublishedFABSFactory(updated_at=date(2024, 2, 4), unique_award_key='record', action_date=date(2023, 3, 22),
                               is_active=True, federal_action_obligation=4.3)
    pf4 = PublishedFABSFactory(updated_at=date(2024, 1, 4), unique_award_key='record', action_date=date(2023, 3, 22),
                               is_active=False, federal_action_obligation=100)
    sess.add_all([pf1, pf2, pf3, pf4])
    sess.commit()

    results = sess.execute(get_award_updates_query('02/03/2024'))
    result_list = results.all()

    assert len(result_list) == 1
    assert result_list[0].base_obligation_date == '2000-03-22'
    assert result_list[0].last_modified_date == '2024-02-04'


def test_range(database):
    sess = database.session
    pf1 = PublishedFABSFactory(updated_at=date(2024, 2, 1), record_type=2, assistance_type='04', fain='tooearly')
    pf2 = PublishedFABSFactory(updated_at=date(2024, 2, 4), record_type=2, assistance_type='04', fain='justright')
    pf3 = PublishedFABSFactory(updated_at=date(2024, 2, 8), record_type=2, assistance_type='04', fain='toolate')
    sess.add_all([pf1, pf2, pf3])
    sess.commit()

    results = sess.execute(get_award_updates_query('02/03/2024', '02/05/2024'))
    result_list = results.all()

    assert len(result_list) == 1
    assert result_list[0].federal_award_id == 'justright'
