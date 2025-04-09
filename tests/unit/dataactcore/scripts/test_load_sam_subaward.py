import itertools
import math
import random
import uuid
from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from dataactcore.models.subaward import SAMSubgrant, SAMSubcontract
from dataactcore.scripts.pipeline.load_sam_subaward import (
    ASSISTANCE_API_URL,
    delete_subawards,
    LIMIT,
    load_subawards,
    parse_raw_subaward,
    pull_subawards,
    store_subawards,
)


class MockSAMSubawardApi:

    def __init__(self, total_publish_records, total_delete_records=0):
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
        total_pages = math.ceil(total_records / page_size)
        return {
            'totalPages': total_pages,
            'totalRecords': total_records,
            'pageNumber': page_number,
            'data': [
                record_type(str(i), i, load_type)
                for i in range(max(0, min(page_size, total_records - (page_number * page_size))))
            ]
        }

    @staticmethod
    def assistance_record(report_number, report_id, load_type='Published'):
        return {
            'status': load_type,
            'submittedDate': date.today().strftime("%Y-%m-%d"),
            'subVendorName': uuid.uuid4().hex,
            'subVendorUei': uuid.uuid4().hex,
            'subAwardNumber': uuid.uuid4().hex,
            'subAwardAmount': uuid.uuid4().hex,
            'subAwardDate': date.today().strftime("%Y-%m-%d"),
            'reportUpdatedDate': date.today().strftime("%Y-%m-%d"),
            'subawardReportId': report_id,
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
    def contract_record(report_number, report_id, load_type=None):
        return {
            "primeContractKey": None,
            "piid": uuid.uuid4().hex,
            "agencyId": uuid.uuid4().hex,
            "referencedIDVPIID": uuid.uuid4().hex,
            "referencedIDVAgencyId": uuid.uuid4().hex,
            "subAwardReportId": report_id,
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
    mock_get_with_exception_hand.side_effect = MockSAMSubawardApi(
        total_publish_records, total_delete_records
    ).get_records
    session = database.session

    # Load subawards
    result = load_subawards(session, data_type=data_type, load_type="published")

    # Check that the length of the result matches the total published records from the api
    # and the number of records in the database.
    assert len(result) == total_publish_records == session.query(model).count()

    # Check that the subaward report numbers in the result match the database
    db_report_nums_post_load = set(record[0] for record in session.query(model.subaward_report_number).all())
    assert set(result) == db_report_nums_post_load

    if load_type == "deleted":

        # Delete subawards if load_type is "deleted"
        delete_result = set(load_subawards(session, data_type=data_type, load_type="deleted"))

        # Check that the length of the delete result match the total delete records in the api response
        assert len(delete_result) == total_delete_records

        # Check that the database has the expected number of records after delete
        assert total_publish_records - total_delete_records == session.query(model).count()

        # Check that none of the deleted report numbers are in the database
        db_report_nums_post_delete = set(record[0] for record in session.query(model.subaward_report_number).all())
        assert delete_result.isdisjoint(db_report_nums_post_delete)

        # Check that the report numbers in the delete result match the difference of the original records from the db
        # and the records remaining in the db post delete
        assert db_report_nums_post_load.difference(db_report_nums_post_delete) == delete_result


@pytest.mark.parametrize("data_type", ("assistance", "contract"))
def test_store_subawards(data_type, database):
    model = SAMSubgrant if data_type == 'assistance' else SAMSubcontract
    new_subawards = pd.DataFrame({'subaward_report_number': [uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex]})
    session = database.session

    # Load subawards.
    store_subawards(session, new_subawards, model)
    db_report_nums_post_load = set(record[0] for record in session.query(model.subaward_report_number).all())

    # Check that all of the subawards are present in the database
    assert session.query(model).count() == len(new_subawards)
    assert set(new_subawards.subaward_report_number) == db_report_nums_post_load

    # Load additional subawards.
    more_subawards = pd.concat([
        new_subawards,
        pd.DataFrame({'subaward_report_number': [uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex]}),
    ])
    store_subawards(session, more_subawards, model)
    db_report_nums_post_load = set(record[0] for record in session.query(model.subaward_report_number).all())

    # The original subawards should be deleted and then reinserted (e.g. no duplicates)
    assert session.query(model).count() == len(more_subawards)
    assert set(more_subawards.subaward_report_number) == db_report_nums_post_load


@pytest.mark.parametrize("data_type", ("assistance", "contract"))
def test_delete_subawards(data_type, database):
    model = SAMSubgrant if data_type == 'assistance' else SAMSubcontract
    new_subawards = pd.DataFrame({'subaward_report_number': [uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex]})
    session = database.session

    # Load subawards
    store_subawards(session, new_subawards, model)
    assert session.query(model).count() == len(new_subawards)

    # Delete subawards that were just loaded
    not_matched_report_nums = delete_subawards(session, new_subawards, model)
    assert session.query(model).count() == 0
    assert not_matched_report_nums == []

    # Attempt to delete subawards with no matches.  Expect to get all ids back in the list of not_matched_report_nums
    not_matched_report_nums = delete_subawards(session, new_subawards, model)
    assert set(not_matched_report_nums) == set(new_subawards.subaward_report_number)


@pytest.fixture()
def parsed_assistance_keys():
    return set(
        col.name
        for col in SAMSubgrant.__table__.columns
        if col.name not in ['created_at', 'updated_at', 'sam_subgrant_id']
    )


@pytest.fixture()
def parsed_contract_keys():
    return set(
        col.name
        for col in SAMSubcontract.__table__.columns
        if col.name not in ['created_at', 'updated_at', 'sam_subcontract_id']
    )


def test_parse_raw_subaward_assistance(parsed_assistance_keys):
    report_number = uuid.uuid4().hex
    report_id = random.randint(1, 1000)
    raw_subaward_dict = MockSAMSubawardApi.assistance_record(report_number, report_id)
    result = parse_raw_subaward(raw_subaward_dict, 'assistance')
    assert set(result.keys()) == parsed_assistance_keys


def test_parse_raw_subaward_contract(parsed_contract_keys):
    report_number = uuid.uuid4().hex
    report_id = random.randint(1, 1000)
    raw_subaward_dict = MockSAMSubawardApi.contract_record(report_number, report_id)
    result = parse_raw_subaward(raw_subaward_dict, 'contract')
    assert set(result.keys()) == parsed_contract_keys


@patch("dataactcore.scripts.pipeline.load_sam_subaward.get_with_exception_hand")
def test_pull_subawards(mock_get_with_exception_hand):
    api = MockSAMSubawardApi(499)
    mock_get_with_exception_hand.side_effect = api.get_records
    subawards = [
        subaward
        for response in pull_subawards(ASSISTANCE_API_URL, {"pageSize": LIMIT, "status": "Published"})
        for subaward in response.get("data", [])
    ]
    assert len(subawards) == 499
    subawards = [
        subaward
        for response in pull_subawards(ASSISTANCE_API_URL, {"pageSize": 200, "status": "Published"})
        for subaward in response.get("data", [])
    ]
    assert len(subawards) == 499
    subawards = [
        subaward
        for response in pull_subawards(
            ASSISTANCE_API_URL,
            {"pageSize": 150, "status": "Published"},
            entries_processed=300,
        )
        for subaward in response.get("data", [])
    ]
    assert len(subawards) == 199
