from unittest.mock import Mock

from dataactvalidator.filestreaming import csvAbstractReader


def test_count_and_set_headers_flex():
    """Verify that we are setting the correct flex headers"""
    reader = csvAbstractReader.CsvAbstractReader()
    csv_schema = [Mock(name_short='some_col'), Mock(name_short='other')]
    header_row = ['ignored', 'flex_my_col', 'some_col', 'flex_other',
                  'some_col']

    result = reader.count_and_set_headers(csv_schema, header_row)
    assert result == {'some_col': 2, 'other': 0}
    assert reader.expected_headers == [
        None, None, 'some_col', None, 'some_col']
    assert reader.flex_headers == [
        None, 'flex_my_col', None, 'flex_other', None]
