from dataactcore.utils import fileF
from tests.unit.dataactcore.factories.fsrs import (FSRSGrantFactory, FSRSProcurementFactory, FSRSSubcontractFactory,
                                                   FSRSSubgrantFactory)
from tests.unit.dataactcore.factories.staging import AwardFinancialAssistanceFactory, AwardProcurementFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory


def test_copy_values_procurement():
    model_row = fileF.ModelRow(None, FSRSProcurementFactory(duns='DUNS'), FSRSSubcontractFactory(duns='DUNS SUB'),
                               None, None)
    mapper = fileF.CopyValues(procurement='duns')
    assert mapper(model_row) == 'DUNS'
    mapper = fileF.CopyValues(subcontract='duns')
    assert mapper(model_row) == 'DUNS SUB'
    mapper = fileF.CopyValues(grant='duns')
    assert mapper(model_row) is None


def test_copy_values_grant():
    model_row = fileF.ModelRow(None, None, None, FSRSGrantFactory(duns='DUNS'), FSRSSubgrantFactory(duns='DUNS SUB'))
    mapper = fileF.CopyValues(grant='duns')
    assert mapper(model_row) == 'DUNS'
    mapper = fileF.CopyValues(subgrant='duns')
    assert mapper(model_row) == 'DUNS SUB'
    mapper = fileF.CopyValues(procurement='duns')
    assert mapper(model_row) is None


def test_country_name():
    model_row = fileF.ModelRow(
        None, None, None, None, FSRSSubgrantFactory(awardee_address_country='USA', principle_place_country='DE')
    )
    entity = fileF.mappings['LegalEntityCountryName'](model_row)
    assert entity == 'United States'

    place = fileF.mappings['PrimaryPlaceOfPerformanceCountryName'](model_row)
    assert place == 'Germany'


def test_zipcode_guard():
    model_row = fileF.ModelRow(
        None, None,
        FSRSSubcontractFactory(company_address_country='USA', company_address_zip='12345'),
        None, None
    )
    us_zip = fileF.mappings['LegalEntityZIP+4'](model_row)
    foreign_zip = fileF.mappings['LegalEntityForeignPostalCode'](model_row)
    assert us_zip == '12345'
    assert foreign_zip is None

    model_row.subcontract.company_address_country = 'RU'
    us_zip = fileF.mappings['LegalEntityZIP+4'](model_row)
    foreign_zip = fileF.mappings['LegalEntityForeignPostalCode'](model_row)
    assert us_zip is None
    assert foreign_zip == '12345'


def test_generate_f_rows(database, monkeypatch):
    """generate_f_rows should find and convert subaward data relevant to a
    specific submission id. We'll compare the resulting DUNs values for
    uniqueness"""
    # Setup - create awards, procurements/grants, subawards
    sess = database.session
    sub_1 = SubmissionFactory()
    sub_2 = SubmissionFactory()
    sess.add_all([sub_1, sub_2])
    sess.commit()

    awards = [AwardProcurementFactory(submission_id=sub_1.submission_id, piid='PIID1', parent_award_id='PIID1'),
              AwardProcurementFactory(submission_id=sub_1.submission_id, piid='PIID2', parent_award_id='PIID2'),
              AwardFinancialAssistanceFactory(submission_id=sub_1.submission_id, fain='FAIN1'),
              AwardFinancialAssistanceFactory(submission_id=sub_1.submission_id, fain='FAIN2'),
              AwardProcurementFactory(submission_id=sub_2.submission_id, piid='PIID1', parent_award_id='PIID1'),
              AwardFinancialAssistanceFactory(submission_id=sub_2.submission_id, fain='FAIN1')]
    sess.add_all(awards)
    procurements = {}
    for piid in ('PIID1', 'PIID2', 'PIID3'):
        procurements[piid] = [
            FSRSProcurementFactory(contract_number=piid, idv_reference_number=piid,
                                   subawards=[FSRSSubcontractFactory() for _ in range(3)]),
            FSRSProcurementFactory(contract_number=piid, idv_reference_number=piid,
                                   subawards=[]),
            FSRSProcurementFactory(contract_number=piid, idv_reference_number=piid,
                                   subawards=[FSRSSubcontractFactory() for _ in range(2)])
        ]
        sess.add_all(procurements[piid])
    grants = {}
    for fain in ('FAIN0', 'FAIN1'):
        grants[fain] = [
            FSRSGrantFactory(fain=fain, subawards=[FSRSSubgrantFactory() for _ in range(3)]),
            FSRSGrantFactory(fain=fain, subawards=[]),
            FSRSGrantFactory(fain=fain, subawards=[FSRSSubgrantFactory() for _ in range(2)])
        ]
        sess.add_all(grants[fain])
    sess.commit()

    actual = {result['SubAwardeeOrRecipientUniqueIdentifier'] for result in fileF.generate_f_rows(sub_1.submission_id)}
    expected = set()
    expected.update(sub.duns for proc in procurements['PIID1'] for sub in proc.subawards)
    expected.update(sub.duns for proc in procurements['PIID2'] for sub in proc.subawards)
    expected.update(sub.duns for grant in grants['FAIN1'] for sub in grant.subawards)
    assert actual == expected


def test_generate_f_rows_naics_desc(database, monkeypatch):
    """The NAICS description should be retireved from an AwardProcurement"""
    sub = SubmissionFactory()
    database.session.add(sub)
    database.session.commit()

    award = AwardProcurementFactory(submission_id=sub.submission_id)
    other_aps = [AwardProcurementFactory(submission_id=award.submission_id) for _ in range(3)]
    proc = FSRSProcurementFactory(contract_number=award.piid, idv_reference_number=award.parent_award_id,
                                  subawards=[FSRSSubcontractFactory(naics=award.naics)])

    database.session.add_all([award, proc] + other_aps)
    database.session.commit()

    actual = {result['NAICS_Description'] for result in fileF.generate_f_rows(award.submission_id)}
    assert actual == {award.naics_description}


def test_generate_f_rows_false(database, monkeypatch):
    """Make sure we're converting False to a string"""
    sub = SubmissionFactory()
    database.session.add(sub)
    database.session.commit()

    award = AwardProcurementFactory(submission_id=sub.submission_id)
    proc = FSRSProcurementFactory(
        contract_number=award.piid,
        idv_reference_number=award.parent_award_id,
        subawards=[FSRSSubcontractFactory(recovery_model_q1=False, recovery_model_q2=None)]
    )

    database.session.add_all([award, proc])
    database.session.commit()

    results = list(fileF.generate_f_rows(award.submission_id))
    assert results[0]['RecModelQuestion1'] == 'False'
    assert results[0]['RecModelQuestion2'] == ''
