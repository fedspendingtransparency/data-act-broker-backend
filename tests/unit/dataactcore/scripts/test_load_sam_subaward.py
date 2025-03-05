import itertools
import random
import uuid
from datetime import date
from unittest.mock import patch

import pytest

from dataactcore.models.fsrs import SAMSubgrant, SAMSubcontract
from dataactcore.scripts.pipeline.load_sam_subaward import  load_subawards

def mock_assistance_response(total_records, include_data=True):
    return {
        'totalPages': 1,
        'totalRecords': total_records,
        'pageNumber': 0,
        'data': [
            {
                'status': 'Published',
                'submittedDate': date.today().strftime("%Y-%m-%d"),
                'subVendorName': uuid.uuid4().hex,
                'subVendorUei': uuid.uuid4().hex,
                'subAwardNumber': uuid.uuid4().hex,
                'subAwardAmount': uuid.uuid4().hex,
                'subAwardDate': date.today().strftime("%Y-%m-%d"),
                'reportUpdatedDate': date.today().strftime("%Y-%m-%d"),
                'subawardReportId': random.randint(1, 1000),
                'subawardReportNumber': uuid.uuid4().hex,
                'placeOfPerformance': {
                    'streetAddress': None,
                    'streetAddress2': None,
                    'city': uuid.uuid4().hex,
                    'congressionalDistrict': uuid.uuid4().hex,
                    'state': {'code': uuid.uuid4().hex, 'name': uuid.uuid4().hex},
                    'country': {'code': uuid.uuid4().hex, 'name': uuid.uuid4().hex},
                    'zip': uuid.uuid4().hex,
                },
                'organizationInfo': None,
                'assistanceListingNumber': [{'title': uuid.uuid4().hex, 'number': uuid.uuid4().hex}],
                'subawardDescription': uuid.uuid4().hex,
                'fain': uuid.uuid4().hex,
                'actionDate': date.today().strftime("%Y-%m-%d"),
                'totalFedFundingAmount': uuid.uuid4().hex,
                'baseObligationDate': date.today().strftime("%Y-%m-%d"),
                'projectDescription': uuid.uuid4().hex,
                'baseAssistanceTypeCode': uuid.uuid4().hex,
                'baseAssistanceTypeDesc': None,
                'agencyCode': uuid.uuid4().hex,
                'assistanceType': None,
                'primeEntityUei': uuid.uuid4().hex,
                'primeEntityName': uuid.uuid4().hex,
                'primeAwardKey': uuid.uuid4().hex,
                'vendorPhysicalAddress': {
                    'streetAddress': uuid.uuid4().hex,
                    'streetAddress2': None,
                    'city': uuid.uuid4().hex,
                    'congressionalDistrict': uuid.uuid4().hex,
                    'state': {'code': uuid.uuid4().hex, 'name': uuid.uuid4().hex},
                    'country': {'code': uuid.uuid4().hex, 'name': uuid.uuid4().hex},
                    'zip': uuid.uuid4().hex,
                },
                'subDbaName': None,
                'subParentName': uuid.uuid4().hex,
                'subParentUei': uuid.uuid4().hex,
                'subBusinessType': [
                    {'code': uuid.uuid4().hex, 'name': uuid.uuid4().hex}
                ],
                'subTopPayEmployee': []
            }
            for _ in range(total_records)
        ] if include_data else []

    }

def mock_contract_response(total_records, include_data=True):
    return {
        'totalPages': 1,
        'totalRecords': total_records,
        'pageNumber': 0,
        'data': [
            {
                "primeContractKey": None,
                "piid": uuid.uuid4().hex,
                "agencyId": uuid.uuid4().hex,
                "referencedIDVPIID": uuid.uuid4().hex,
                "referencedIDVAgencyId": uuid.uuid4().hex,
                "subAwardReportId": random.randint(1, 1000),
                "subAwardReportNumber": uuid.uuid4().hex,
                "submittedDate": date.today().strftime("%Y-%m-%d"),
                "subAwardNumber": uuid.uuid4().hex,
                "subAwardAmount": uuid.uuid4().hex,
                "subAwardDate": date.today().strftime("%Y-%m-%d"),
                "subEntityLegalBusinessName": uuid.uuid4().hex,
                "subEntityUei": uuid.uuid4().hex,
                "primeAwardType": None,
                "totalContractValue": uuid.uuid4().hex,
                "primeEntityUei": uuid.uuid4().hex,
                "primeEntityName": uuid.uuid4().hex,
                "baseAwardDateSigned": date.today().strftime("%Y-%m-%d"),
                "descriptionOfRequirement": uuid.uuid4().hex,
                "primeNaics": {
                    "code": uuid.uuid4().hex,
                    "description": uuid.uuid4().hex,
                },
                "primeOrganizationInfo": uuid.uuid4().hex,
                "entityPhysicalAddress": {
                    "streetAddress": uuid.uuid4().hex,
                    "streetAddress2": None,
                    "city": uuid.uuid4().hex,
                    "congressionalDistrict": None,
                    "state": {
                        "code": uuid.uuid4().hex,
                        "name": uuid.uuid4().hex,
                    },
                    "country": {
                        "code": uuid.uuid4().hex,
                        "name": uuid.uuid4().hex,
                    },
                    "zip": uuid.uuid4().hex,
                },
                "subBusinessType": None,
                "subParentUei": uuid.uuid4().hex,
                "subawardDescription": uuid.uuid4().hex,
                "subEntityDoingBusinessAsName": None,
                "subTopPayEmployee": [
                    {"salary": uuid.uuid4().hex, "fullname": uuid.uuid4().hex},
                    {"salary": uuid.uuid4().hex, "fullname": uuid.uuid4().hex},
                    {"salary": uuid.uuid4().hex, "fullname": uuid.uuid4().hex},
                    {"salary": uuid.uuid4().hex, "fullname": uuid.uuid4().hex},
                    {"salary": uuid.uuid4().hex, "fullname": uuid.uuid4().hex},
                ],
                "subEntityParentLegalBusinessName": uuid.uuid4().hex,
            }
            for _ in range(total_records)
        ] if include_data else []
    }

@pytest.mark.parametrize("data_type,load_type", itertools.product(("assistance", "contract"), ("published", "deleted")))
@patch("dataactcore.scripts.pipeline.load_sam_subaward.get_with_exception_hand")
def test_load_subawards(mock_get_with_exception_hand, data_type, load_type, database):
    test_config = {
        "assistance": {
            "mock_fn": mock_assistance_response,
            "model": SAMSubgrant,
            "report_num_key": 'subawardReportNumber'
        },
        "contract": {
            "mock_fn": mock_contract_response,
            "model": SAMSubcontract,
            "report_num_key": 'subAwardReportNumber'
        }
    }
    config = test_config[data_type]
    total_responses = 100
    response = config["mock_fn"](total_responses)
    mock_get_with_exception_hand.side_effect = (
        [response] * 2 # first two responses should include the data
        + [config["mock_fn"](total_responses, False)] * (total_responses - 2)
    )
    session = database.session
    result = load_subawards(session, data_type=data_type, load_type="published")
    assert len(result) == total_responses
    assert set(result) == {record[config["report_num_key"]] for record in response['data']}
    assert session.query(config["model"]).count() == total_responses
    if load_type == "deleted":
        # Select half the data from the response to delete
        delete_response = {**response, **{"totalRecords": 50, "data": response["data"][::2]}}
        mock_get_with_exception_hand.side_effect = (
                [delete_response] * 2  # first two responses should include the data
                + [config["mock_fn"](total_responses, False)] * (total_responses - 2)
        )
        delete_result = load_subawards(session, data_type=data_type, load_type="deleted")
        assert len(delete_result) == total_responses / 2
        assert set(delete_result) == {record[config["report_num_key"]] for record in delete_response['data']}
        assert session.query(config["model"]).count() == total_responses / 2
