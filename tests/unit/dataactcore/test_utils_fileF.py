from unittest.mock import Mock

from dataactcore.utils import fileF
from tests.unit.dataactcore.factories.fsrs import (
    FSRSGrantFactory, FSRSProcurementFactory, FSRSSubcontractFactory,
    FSRSSubgrantFactory)
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory


def test_CopyValues_procurement():
    proc = FSRSProcurementFactory(duns='DUNS')
    sub = FSRSSubcontractFactory(duns='DUNS SUB')
    mapper = fileF.CopyValues(procurement='duns')
    assert mapper.subcontract(proc, sub) == 'DUNS'
    mapper = fileF.CopyValues(subcontract='duns')
    assert mapper.subcontract(proc, sub) == 'DUNS SUB'
    mapper = fileF.CopyValues(grant='duns')
    assert mapper.subcontract(proc, sub) == ''


def test_CopyValues_grant():
    grant = FSRSGrantFactory(duns='DUNS')
    sub = FSRSSubgrantFactory(duns='DUNS SUB')
    mapper = fileF.CopyValues(grant='duns')
    assert mapper.subgrant(grant, sub) == 'DUNS'
    mapper = fileF.CopyValues(subgrant='duns')
    assert mapper.subgrant(grant, sub) == 'DUNS SUB'
    mapper = fileF.CopyValues(procurement='duns')
    assert mapper.subgrant(grant, sub) == ''


def test_relevantFainPiids(database):
    """We should be retrieving a pair of all FAINs and PIIDs associated with a
    particular submission"""
    sess = database.session
    award1 = AwardFinancialFactory()
    award2 = AwardFinancialFactory(submission_id=award1.submission_id,
                                   piid=None)
    award3 = AwardFinancialFactory()
    sess.add_all([award1, award2, award3])

    fains, piids = fileF.relevantFainsPiids(sess, award1.submission_id)
    assert fains == {award1.fain, award2.fain}  # ignores award3
    assert piids == {award1.piid}               # ignores award2's None


def test_generateFRows(database, monkeypatch):
    """generateFRows should find and convert subaward data relevant to a
    specific submission id. We'll compare the resulting DUNs values for
    uniqueness"""
    sess = database.session

    mock_fn = Mock(return_value=({'fain1', 'fain2'}, {'piid1'}))
    monkeypatch.setattr(fileF, 'relevantFainsPiids', mock_fn)
    # Create some dummy data: 4 procurements, 4 grants, each with 3 subawards
    procs = [FSRSProcurementFactory(contract_number='piid' + str(i))
             for i in range(0, 4)]
    for proc in procs:
        proc.subawards = [FSRSSubcontractFactory() for _ in range(3)]
    grants = [FSRSGrantFactory(fain='fain' + str(i)) for i in range(0, 4)]
    for grant in grants:
        grant.subawards = [FSRSSubgrantFactory() for _ in range(3)]

    sess.add_all(procs + grants)

    actual = {result['SubAwardeeOrRecipientUniqueIdentifier']
              for result in fileF.generateFRows(sess, 1234)}
    expected = set()
    for award in procs[1:3] + grants[1:2]:
        expected.update(sub.duns for sub in award.subawards)
    assert actual == expected
    # Also make sure that we filtered by the right submission
    assert mock_fn.call_args == ((sess, 1234),)
