import itertools
import random
import uuid
from datetime import date
from unittest.mock import patch

import pytest

from dataactcore.models.fsrs import SAMSubgrant, SAMSubcontract
from dataactcore.scripts.pipeline.load_sam_subaward import ASSISTANCE_API_URL, LIMIT, load_subawards


class MockApi:

    def __init__(self, total_publish_records, total_delete_records):
        self.total_publish_records = total_publish_records
        self.total_delete_records = total_delete_records

    def get_records(self, url_string):
        base_url = url_string.split('?')[0]
        if base_url == ASSISTANCE_API_URL.split('?')[0]:
            record_type = self.assistance_record
        else:
            record_type = self.contract_record
        params_strings = url_string.split('?')[1].split('&')
        params = {
            params_string.split('=')[0]: params_string.split('=')[1]
            for params_string in params_strings
        }
        page_size = int(params.get('pageSize', LIMIT))
        page_number = int(params.get('pageNumber', 0))
        load_type = params.get('status')
        total_records = self.total_publish_records if load_type == 'Published' else self.total_delete_records
        return {
            'totalPages': max(1, total_records // page_size),
            'totalRecords': total_records,
            'pageNumber': page_number,
            'data': [
                record_type(str(i), load_type)
                for i in range(max(0, total_records - (page_size * page_number)))
            ]
        }

    @staticmethod
    def assistance_record(report_number, load_type='Published'):
        return {
            'status': load_type,
            'submittedDate': date.today().strftime("%Y-%m-%d"),
            'subVendorName': uuid.uuid4().hex,
            'subVendorUei': uuid.uuid4().hex,
            'subAwardNumber': uuid.uuid4().hex,
            'subAwardAmount': uuid.uuid4().hex,
            'subAwardDate': date.today().strftime("%Y-%m-%d"),
            'reportUpdatedDate': date.today().strftime("%Y-%m-%d"),
            'subawardReportId': random.randint(1, 1000),
            'subawardReportNumber': report_number,
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

    @staticmethod
    def contract_record(report_number, load_type=None):
        return  {
            "primeContractKey": None,
            "piid": uuid.uuid4().hex,
            "agencyId": uuid.uuid4().hex,
            "referencedIDVPIID": uuid.uuid4().hex,
            "referencedIDVAgencyId": uuid.uuid4().hex,
            "subAwardReportId": random.randint(1, 1000),
            "subAwardReportNumber": report_number,
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


@pytest.mark.parametrize("data_type,load_type", itertools.product(("assistance", "contract"), ("published", "deleted")))
@patch("dataactcore.scripts.pipeline.load_sam_subaward.get_with_exception_hand")
def test_load_subawards(mock_get_with_exception_hand, data_type, load_type, database):
    model = SAMSubgrant if data_type == 'assistance' else SAMSubcontract
    total_publish_records = 100
    total_delete_records = 50
    mock_get_with_exception_hand.side_effect = MockApi(total_publish_records, total_delete_records).get_records
    session = database.session
    result = load_subawards(session, data_type=data_type, load_type="published")
    assert len(result) == total_publish_records
    assert len(result) == session.query(model).count()
    db_report_nums_post_load = set(record[0] for record in session.query(model.subaward_report_number).all())
    assert set(result) == db_report_nums_post_load
    if load_type == "deleted":
        delete_result = set(load_subawards(session, data_type=data_type, load_type="deleted"))
        db_report_nums_post_delete = set(record[0] for record in session.query(model.subaward_report_number).all())
        assert len(delete_result) == total_delete_records
        assert total_publish_records - total_delete_records == session.query(model).count()
        assert delete_result.isdisjoint(db_report_nums_post_delete)
        assert db_report_nums_post_load.difference(db_report_nums_post_delete) == delete_result
