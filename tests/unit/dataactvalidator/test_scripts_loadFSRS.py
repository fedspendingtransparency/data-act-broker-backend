from unittest.mock import Mock

import pytest

from dataactcore.models.fsrs import FSRSAward, FSRSSubaward
from dataactvalidator.scripts import loadFSRS
from tests.unit.dataactcore.factories import (
    FSRSAwardFactory, FSRSSubawardFactory)


@pytest.fixture()
def no_award_db(database):
    sess = database.validationDb.session
    sess.query(FSRSAward).delete(synchronize_session=False)
    sess.commit()

    yield sess

    sess.query(FSRSAward).delete(synchronize_session=False)
    sess.commit()


@pytest.mark.usesfixtures(['no_award_db'])
def test_maxCurrentId_default():
    assert -1 == loadFSRS.maxCurrentId()


def test_maxCurrentId(no_award_db):
    no_award_db.add_all([FSRSAwardFactory(id=5),
                         FSRSAwardFactory(id=2)])
    no_award_db.commit()

    assert 5 == loadFSRS.maxCurrentId()


def test_loadFSRS_saves_data(no_award_db, monkeypatch):
    award1 = FSRSAwardFactory()
    award1.subawards = [FSRSSubawardFactory() for _ in range(4)]
    award2 = FSRSAwardFactory()
    award2.subawards = [FSRSSubawardFactory()]
    monkeypatch.setattr(loadFSRS, 'retrieveAwardsBatch',
                        Mock(return_value=[award1, award2]))

    assert no_award_db.query(FSRSAward).count() == 0
    loadFSRS.loadFSRS(0)
    assert no_award_db.query(FSRSAward).count() == 2
    assert no_award_db.query(FSRSSubaward).count() == 5


def test_loadFSRS_overrides_data(no_award_db, monkeypatch):
    def fetch_duns(award_id):
        return no_award_db.query(FSRSAward.duns).filter(
            FSRSAward.id == award_id).one()[0]

    award1 = FSRSAwardFactory(id=1, duns='Not Altered')
    award1.subawards = [FSRSSubawardFactory() for _ in range(4)]
    award2 = FSRSAwardFactory(id=2, duns='To Be Replaced')
    award2.subawards = [FSRSSubawardFactory()]
    monkeypatch.setattr(loadFSRS, 'retrieveAwardsBatch',
                        Mock(return_value=[award1, award2]))

    loadFSRS.loadFSRS(0)
    assert fetch_duns(1) == 'Not Altered'
    assert fetch_duns(2) == 'To Be Replaced'
    assert no_award_db.query(FSRSSubaward).count() == 5

    award3 = FSRSAwardFactory(id=2, duns='Replacing')
    monkeypatch.setattr(loadFSRS, 'retrieveAwardsBatch',
                        Mock(return_value=[award3]))
    loadFSRS.loadFSRS(0)
    assert fetch_duns(1) == 'Not Altered'
    assert fetch_duns(2) == 'Replacing'
    assert no_award_db.query(FSRSSubaward).count() == 4
