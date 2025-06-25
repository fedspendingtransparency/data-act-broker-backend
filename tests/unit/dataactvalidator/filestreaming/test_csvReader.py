from unittest.mock import Mock

from dataactvalidator.filestreaming import csvReader


def test_count_and_set_headers_flex():
    """Verify that we are setting the correct flex headers"""
    reader = csvReader.CsvReader()
    csv_schema = [Mock(name_short="some_col"), Mock(name_short="other")]
    header_row = ["ignored", "flex_my_col", "flex test", "space col", "some_col", "flex_other", "some_col"]

    result = reader.count_and_set_headers(csv_schema, header_row)
    assert result == {"some_col": 2, "other": 0}
    assert reader.expected_headers == [None, None, None, None, "some_col", None, "some_col"]
    assert reader.flex_headers == [None, "flex_my_col", None, None, None, "flex_other", None]


def test_normalize_headers():
    """Verify we return the transformed headers depending on the long_headers parameter and that special exceptions
    are processed correctly.
    """
    # Verify names are properly lowercased and not mapped if long_headers is false
    headers = ["AllocationTransferAgencyIdentifier", "BeginningPeriodOfAvailability", "flex_mycol", "FLEX_ANOTHER"]
    mapping = {"allocationtransferagencyidentifier": "ata", "beginningperiodofavailability": "boa"}

    result = csvReader.normalize_headers(headers, False, mapping, {})
    assert list(result) == [
        "allocationtransferagencyidentifier",
        "beginningperiodofavailability",
        "flex_mycol",
        "flex_another",
    ]

    # Verify names are properly lowercased and mapped to short names if long_headers is true
    result = csvReader.normalize_headers(headers, True, mapping, {})
    assert list(result) == ["ata", "boa", "flex_mycol", "flex_another"]

    # Verify that special hardcoded exceptions are properly handled
    headers = [
        "deobligationsrecoveriesrefundsofprioryearbyprogramobjectclass_cpe",
        "facevalueloanguarantee",
        "budgetauthorityavailableamounttotal_cpe",
        "CorrectionLateDeleteIndicator",
        "place_of_performance_zip4",
    ]
    mapping = {
        "deobligationsrecoveriesrefundsdofprioryearbyprogramobjectclass_cpe": "drfpbpo",
        "facevalueofdirectloanorloanguarantee": "fvdllg",
        "totalbudgetaryresources_cpe": "tbr",
        "correctiondeleteindicator": "cdi",
        "place_of_performance_zip4a": "zip4a",
    }

    # Test for long special headers to be properly mapped
    result = csvReader.normalize_headers(headers, False, mapping, {})
    assert list(result) == [
        "deobligationsrecoveriesrefundsdofprioryearbyprogramobjectclass_cpe",
        "facevalueofdirectloanorloanguarantee",
        "totalbudgetaryresources_cpe",
        "correctiondeleteindicator",
        "place_of_performance_zip4a",
    ]

    # Test for short special headers to be properly mapped
    result = csvReader.normalize_headers(headers, True, mapping, {})
    assert list(result) == ["drfpbpo", "fvdllg", "tbr", "cdi", "zip4a"]

    # Verify names are not mapped if they include extra characters (spaces, parentheses, etc.)
    headers = ["Allocation Transfer Agency Identifier", "FLEX(ANOTHER)", "LegalEntityZip+4"]
    mapping = {"allocationtransferagencyidentifier": "ata", "legalentityzip+4": "legal_entity_zip4"}

    result = csvReader.normalize_headers(headers, True, mapping, {})
    assert list(result) == ["allocation transfer agency identifier", "flex(another)", "legal_entity_zip4"]

    headers = ["ata identifier", "legal entity zip4"]
    mapping = {"ata_identifier": "ata", "legalentityzip+4": "legal_entity_zip4"}

    result = csvReader.normalize_headers(headers, False, mapping, {})
    assert list(result) == headers
