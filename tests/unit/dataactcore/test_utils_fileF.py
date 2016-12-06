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
    assert mapper.subcontract(proc, sub) is None


def test_CopyValues_grant():
    grant = FSRSGrantFactory(duns='DUNS')
    sub = FSRSSubgrantFactory(duns='DUNS SUB')
    mapper = fileF.CopyValues(grant='duns')
    assert mapper.subgrant(grant, sub) == 'DUNS'
    mapper = fileF.CopyValues(subgrant='duns')
    assert mapper.subgrant(grant, sub) == 'DUNS SUB'
    mapper = fileF.CopyValues(procurement='duns')
    assert mapper.subgrant(grant, sub) is None


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


def test_country_name():
    sub = FSRSSubgrantFactory(awardee_address_country='USA',
                              principle_place_country='DE')
    entity = fileF.mappings['LegalEntityCountryName'].subgrant(subgrant=sub)
    assert entity == 'United States'

    place = fileF.mappings['PrimaryPlaceOfPerformanceCountryName'].subgrant(
        subgrant=sub)
    assert place == 'Germany'


def test_zipcode_guard():
    sub = FSRSSubcontractFactory(company_address_country='USA',
                                 company_address_zip='12345')
    us_zip = fileF.mappings['LegalEntityZIP+4'].subcontract(subcontract=sub)
    foreign_zip = fileF.mappings['LegalEntityForeignPostalCode'].subcontract(
        subcontract=sub)
    assert us_zip == '12345'
    assert foreign_zip is None

    sub.company_address_country = 'RU'
    us_zip = fileF.mappings['LegalEntityZIP+4'].subcontract(subcontract=sub)
    foreign_zip = fileF.mappings['LegalEntityForeignPostalCode'].subcontract(
        subcontract=sub)
    assert us_zip is None
    assert foreign_zip == '12345'


def test_generateFRows(database, monkeypatch):
    """generateFRows should find and convert subaward data relevant to a
    specific submission id. We'll compare the resulting DUNs values for
    uniqueness"""
    sess = database.session

    mock_fn = Mock(return_value=({'fain1', 'fain2'}, {'piid1'}))
    monkeypatch.setattr(fileF, 'relevantFainsPiids', mock_fn)
    # Create some dummy data: 4 procurements, 4 grants, each with 3 subawards
    procs = [FSRSProcurementFactory(contract_number='piid' + str(i))
             for i in range(4)]
    for proc in procs:
        proc.subawards = [FSRSSubcontractFactory() for _ in range(3)]
    grants = [FSRSGrantFactory(fain='fain' + str(i)) for i in range(4)]
    for grant in grants:
        grant.subawards = [FSRSSubgrantFactory() for _ in range(3)]

    sess.add_all(procs + grants)

    actual = {result['SubAwardeeOrRecipientUniqueIdentifier']
              for result in fileF.generateFRows(sess, 1234)}
    # fain1, fain2
    expected = {sub.duns for award in grants[1:3] for sub in award.subawards}
    # piid1
    expected.update(sub.duns for sub in procs[1].subawards)
    assert actual == expected
    # Also make sure that we filtered by the right submission
    assert mock_fn.call_args == ((sess, 1234),)
