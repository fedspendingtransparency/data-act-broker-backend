from collections import OrderedDict

from dataactcore.models.fsrs import (
    FSRSGrant, FSRSProcurement, FSRSSubcontract, FSRSSubgrant)
from dataactcore.models.stagingModels import AwardFinancial


# Value options:    (see valueFromMapping for implementation)
# None - do nothing; a todo
# str - same field is used by both subcontracts and subawards
# (str, str) - pair of fields to use for (subcontract, subawards) if either
#       value is None, no value should be used
mappings = OrderedDict()
mappings['SubAwardeeOrRecipientLegalEntityName'] = ('company_name',
                                                    'awardee_name')
mappings['SubAwardeeOrRecipientUniqueIdentifier'] = 'duns'
mappings['SubAwardeeUltimateParentUniqueIdentifier'] = 'parent_duns'
mappings['SubAwardeeUltimateParentLegalEntityName'] = ('parent_company_name',
                                                       None)  # check
mappings['LegalEntityAddressLine1'] = ('company_address_street',
                                       'awardee_address_street')
mappings['LegalEntityCityName'] = ('company_address_city',
                                   'awardee_address_city')
mappings['LegalEntityStateCode'] = ('company_address_state',
                                    'awardee_address_state')
mappings['LegalEntityZIP+4'] = ('company_address_zip', 'awardee_address_zip')
mappings['LegalEntityForeignPostalCode'] = None     # @todo
mappings['LegalEntityCongressionalDistrict'] = ('company_address_district',
                                                'awardee_address_district')
mappings['LegalEntityCountryCode'] = None   # @todo
mappings['LegalEntityCountryName'] = ('company_address_country',
                                      'awardee_address_country')
mappings['HighCompOfficer1FullName'] = 'top_paid_fullname_1'
mappings['HighCompOfficer1Amount'] = 'top_paid_amount_1'
mappings['HighCompOfficer2FullName'] = 'top_paid_fullname_2'
mappings['HighCompOfficer2Amount'] = 'top_paid_amount_2'
mappings['HighCompOfficer3FullName'] = 'top_paid_fullname_3'
mappings['HighCompOfficer3Amount'] = 'top_paid_amount_3'
mappings['HighCompOfficer4FullName'] = 'top_paid_fullname_4'
mappings['HighCompOfficer4Amount'] = 'top_paid_amount_4'
mappings['HighCompOfficer5FullName'] = 'top_paid_fullname_5'
mappings['HighCompOfficer5Amount'] = 'top_paid_amount_5'
mappings['SubcontractAwardAmount'] = ('subcontract_amount', None)
mappings['TotalFundingAmount'] = (None, 'subaward_amount')
mappings['NAICS'] = ('naics', None)
mappings['NAICS_Description'] = None    # @todo
mappings['CFDA_NumberAndTitle'] = (None, 'cfda_numbers')
mappings['AwardingSubTierAgencyName'] = 'funding_agency_name'     # check
mappings['AwardingSubTierAgencyCode'] = None    # @todo
mappings['AwardDescription'] = ('overall_description', 'project_description')
mappings['ActionDate'] = None   # @todo: grant.obligation_date,
mappings['PrimaryPlaceOfPerformanceCityName'] = 'principle_place_city'
mappings['PrimaryPlaceOfPerformanceAddressLine1'] = 'principle_place_street'
mappings['PrimaryPlaceOfPerformanceStateCode'] = 'principle_place_state'
mappings['PrimaryPlaceOfPerformanceZIP+4'] = 'principle_place_zip'
mappings['PrimaryPlaceOfPerformanceCongressionalDistrict'] = \
        'principle_place_district'
mappings['PrimaryPlaceOfPerformanceCountryCode'] = None     # @todo
mappings['PrimaryPlaceOfPerformanceCountryName'] = 'principle_place_country'
mappings['Vendor Doing As Business Name'] = 'dba_name'
mappings['PrimeAwardReportID'] = None   # @todo contract_number, fain
mappings['ParentAwardId'] = None    # @todo
mappings['AwardReportMonth'] = None     # @todo
mappings['AwardReportYear'] = None  # @todo
mappings['RecModelQuestion1'] = None    # @todo conversion from bool
mappings['RecModelQuestion2'] = None    # @todo conversion from bool
mappings['SubawardNumber'] = None       # @todo
mappings['SubawardeeBusinessType'] = None   # @todo
mappings['AwardeeOrRecipientUniqueIdentifier'] = 'duns'     # check


def valueFromMapping(procurement, subcontract, grant, subgrant, mapping):
    """We configure mappings between FSRS field names and our needs above.
    This function uses that config to derive a value from the provided
    grant/subgrant"""
    subaward = subcontract or subgrant
    if mapping is None:
        return ''
    elif isinstance(mapping, str):
        return getattr(subaward, mapping)
    elif isinstance(mapping, tuple) and subcontract:
        return valueFromMapping(procurement, subcontract, grant, subgrant,
                                mapping[0])
    elif isinstance(mapping, tuple) and subgrant:
        return valueFromMapping(procurement, subcontract, grant, subgrant,
                                mapping[1])
    else:
        raise ValueError("Unknown mapping type: {}".format(mapping))


def relevantFainsPiids(sess, submissionId):
    """Fetch distinct fain and piid values related to this submission"""
    pairs = sess.query(AwardFinancial.fain, AwardFinancial.piid).filter(
        AwardFinancial.submission_id == submissionId)
    fains, piids = set(), set()
    for fain, piid in pairs:
        if fain:
            fains.add(fain)
        if piid:
            piids.add(piid)
    return fains, piids


def generateFRows(sess, submissionId):
    """Generated OrderedDicts representing File F rows. Subawards are filtered
    to those relevant to a particular submissionId"""
    fains, piids = relevantFainsPiids(sess, submissionId)

    query = sess.query(FSRSProcurement, FSRSSubcontract).\
        filter(FSRSProcurement.id == FSRSSubcontract.parent_id).\
        filter(FSRSProcurement.contract_number.in_(piids))
    for proc, sub in query:
        result = OrderedDict()
        for key, mapping in mappings.items():
            result[key] = valueFromMapping(proc, sub, None, None, mapping)
        yield result

    query = sess.query(FSRSGrant, FSRSSubgrant).\
        filter(FSRSGrant.id == FSRSSubgrant.parent_id).\
        filter(FSRSGrant.fain.in_(fains))
    for grant, sub in query:
        result = OrderedDict()
        for key, mapping in mappings.items():
            result[key] = valueFromMapping(None, None, grant, sub, mapping)
        yield result
