from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from tests.unit.dataactcore.factories.domain import ZipsHistoricalFactory

_FILE = 'fabs44_5_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_congressional', 'legal_entity_zip5', 'legal_entity_zip_last4',
                       'action_date', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test if LegalEntityCongressionalDistrict is provided with an ActionDate before 20230103, then the
        LegalEntityCongressionalDistrict should be associated with the provided LegalEntityZIP5 and LegalEntityZIPLast4
        according to the historic USPS source data.
    """
    zips_1 = ZipsHistoricalFactory(zip5='12345', zip_last4='6789', congressional_district_no='01')
    zips_2 = ZipsHistoricalFactory(zip5='12345', zip_last4='9876', congressional_district_no='02')

    fabs_1 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='',
                         legal_entity_congressional='02', correction_delete_indicatr='', action_date='20230102')
    fabs_2 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='6789',
                         legal_entity_congressional='01', correction_delete_indicatr='', action_date='20230102')
    # Test ignoring blank/empty string zips
    fabs_3 = FABSFactory(legal_entity_zip5=None, legal_entity_zip_last4='',
                         legal_entity_congressional='03', correction_delete_indicatr='', action_date='20230102')
    # Ignore dates before 20230103
    fabs_4 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='',
                         legal_entity_congressional='03', correction_delete_indicatr='', action_date='20230104')
    # Ignore CD of 90
    fabs_5 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='',
                         legal_entity_congressional='90', correction_delete_indicatr='', action_date='20230102')
    # Ignore correction delete indicator of D
    fabs_6 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='',
                         legal_entity_congressional='03', correction_delete_indicatr='d', action_date='20230102')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, zips_1, zips_2])
    assert errors == 0


def test_failure(database):
    """ Test failure if LegalEntityCongressionalDistrict is provided with an ActionDate before 20230103, then the
        LegalEntityCongressionalDistrict should be associated with the provided LegalEntityZIP5 and LegalEntityZIPLast4
        according to the historic USPS source data.
    """
    zips = ZipsHistoricalFactory(zip5='12345', zip_last4='6789', congressional_district_no='01')

    fabs_1 = FABSFactory(legal_entity_zip5='54321', legal_entity_zip_last4='',
                         legal_entity_congressional='02', correction_delete_indicatr='', action_date='20230102')
    fabs_2 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='1234',
                         legal_entity_congressional='01', correction_delete_indicatr='', action_date='20230102')
    fabs_3 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None,
                         legal_entity_congressional='03', correction_delete_indicatr='', action_date='20230102')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, zips])
    assert errors == 3
