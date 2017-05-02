from unittest.mock import Mock

from dataactvalidator.filestreaming import csvReader


def test_count_and_set_headers_flex():
    """Verify that we are setting the correct flex headers"""
    reader = csvReader.CsvReader()
    csv_schema = [Mock(name_short='some_col'), Mock(name_short='other')]
    header_row = ['ignored', 'flex_my_col', 'some_col', 'flex_other', 'some_col']

    result = reader.count_and_set_headers(csv_schema, header_row)
    assert result == {'some_col': 2, 'other': 0}
    assert reader.expected_headers == [None, None, 'some_col', None, 'some_col']
    assert reader.flex_headers == [None, 'flex_my_col', None, 'flex_other', None]


def test_get_next_record_flex():
    """Verify that we get a list of FlexFields if present"""
    reader = csvReader.CsvReader()
    reader.delimiter = ','
    reader.column_count = 6
    reader.expected_headers = ['a', 'b', 'c', None, None, None]
    reader.flex_headers = [None, None, None, 'flex_d', 'flex_e', None]
    reader._get_line = lambda: 'A,B,C,D,E,F'
    return_dict, flex_fields = reader.get_next_record()
    assert return_dict == {'a': 'A', 'b': 'B', 'c': 'C'}
    assert len(flex_fields) == 2
    assert flex_fields[0].header == 'flex_d'
    assert flex_fields[0].cell == 'D'
    assert flex_fields[1].header == 'flex_e'
    assert flex_fields[1].cell == 'E'


def test_normalize_headers():
    """Verify we return the transformed headers depending on the long_headers
    parameter"""
    headers = [
        'AllocationTransferAgencyIdentifier', 'BeginningPeriodOfAvailability', 'flex_mycol', 'FLEX_ANOTHER'
    ]
    mapping = {'allocationtransferagencyidentifier': 'ata', 'beginningperiodofavailability': 'boa'}

    result = csvReader.normalize_headers(headers, False, mapping)
    assert list(result) == [
        'allocationtransferagencyidentifier', 'beginningperiodofavailability', 'flex_mycol', 'flex_another'
    ]
    result = csvReader.normalize_headers(headers, True, mapping)
    assert list(result) == ['ata', 'boa', 'flex_mycol', 'flex_another']
