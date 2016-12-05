from collections import OrderedDict

from dataactcore.models.fsrs import (
    FSRSGrant, FSRSProcurement, FSRSSubcontract, FSRSSubgrant)
from dataactcore.models.stagingModels import AwardFinancial


class CopyValues():
    """Copy a field value from one of our existing models"""
    def __init__(self, subcontract=None, subgrant=None, procurement=None,
                 grant=None):
        self.procurement_field = procurement
        self.subcontract_field = subcontract
        self.grant_field = grant
        self.subgrant_field = subgrant

    def subcontract(self, procurement, subcontract):
        if self.subcontract_field:
            return getattr(subcontract, self.subcontract_field)
        elif self.procurement_field:
            return getattr(procurement, self.procurement_field)
        else:
            return ''

    def subgrant(self, grant, subgrant):
        if self.subgrant_field:
            return getattr(subgrant, self.subgrant_field)
        elif self.grant_field:
            return getattr(grant, self.grant_field)
        else:
            return ''


def copy_subaward_field(field_name):
    return CopyValues(field_name, field_name)


def todo():
    return CopyValues()     # noop


mappings = OrderedDict()
mappings['SubAwardeeOrRecipientLegalEntityName'] = CopyValues('company_name',
                                                              'awardee_name')
mappings['SubAwardeeOrRecipientUniqueIdentifier'] = copy_subaward_field('duns')
mappings['SubAwardeeUltimateParentUniqueIdentifier'] = copy_subaward_field('parent_duns')
mappings['SubAwardeeUltimateParentLegalEntityName'] = CopyValues(
    subcontract='parent_company_name')    # @todo nothing for subgrants?
mappings['LegalEntityAddressLine1'] = CopyValues('company_address_street',
                                                 'awardee_address_street')
mappings['LegalEntityCityName'] = CopyValues('company_address_city',
                                             'awardee_address_city')
mappings['LegalEntityStateCode'] = CopyValues('company_address_state',
                                              'awardee_address_state')
mappings['LegalEntityZIP+4'] = CopyValues('company_address_zip',
                                          'awardee_address_zip')
mappings['LegalEntityForeignPostalCode'] = todo()
mappings['LegalEntityCongressionalDistrict'] = CopyValues(
    subcontract='company_address_district', subgrant='awardee_address_district')
mappings['LegalEntityCountryCode'] = todo()
mappings['LegalEntityCountryName'] = CopyValues('company_address_country',
                                                'awardee_address_country')
mappings['HighCompOfficer1FullName'] = copy_subaward_field('top_paid_fullname_1')
mappings['HighCompOfficer1Amount'] = copy_subaward_field('top_paid_amount_1')
mappings['HighCompOfficer2FullName'] = copy_subaward_field('top_paid_fullname_2')
mappings['HighCompOfficer2Amount'] = copy_subaward_field('top_paid_amount_2')
mappings['HighCompOfficer3FullName'] = copy_subaward_field('top_paid_fullname_3')
mappings['HighCompOfficer3Amount'] = copy_subaward_field('top_paid_amount_3')
mappings['HighCompOfficer4FullName'] = copy_subaward_field('top_paid_fullname_4')
mappings['HighCompOfficer4Amount'] = copy_subaward_field('top_paid_amount_4')
mappings['HighCompOfficer5FullName'] = copy_subaward_field('top_paid_fullname_5')
mappings['HighCompOfficer5Amount'] = copy_subaward_field('top_paid_amount_5')
mappings['SubcontractAwardAmount'] = CopyValues(
    subcontract='subcontract_amount')   # @todo  nothing for subgrants?
mappings['TotalFundingAmount'] = CopyValues(
    subgrant='subaward_amount')     # @todo    nothing for subcontracts?
mappings['NAICS'] = CopyValues(
    subcontract='naics')    # @todo    nothing for subgrants?
mappings['NAICS_Description'] = todo()
mappings['CFDA_NumberAndTitle'] = CopyValues(
    subgrant='cfda_numbers')    # @todo     nothing for subcontracts
mappings['AwardingSubTierAgencyName'] = copy_subaward_field(
    'funding_agency_name')     # @todo - check
mappings['AwardingSubTierAgencyCode'] = todo()
mappings['AwardDescription'] = CopyValues('overall_description',
                                          'project_description')
mappings['ActionDate'] = todo()     # grant.obligation_date
mappings['PrimaryPlaceOfPerformanceCityName'] = copy_subaward_field('principle_place_city')
mappings['PrimaryPlaceOfPerformanceAddressLine1'] = copy_subaward_field('principle_place_street')
mappings['PrimaryPlaceOfPerformanceStateCode'] = copy_subaward_field('principle_place_state')
mappings['PrimaryPlaceOfPerformanceZIP+4'] = copy_subaward_field('principle_place_zip')
mappings['PrimaryPlaceOfPerformanceCongressionalDistrict'] = copy_subaward_field('principle_place_district')
mappings['PrimaryPlaceOfPerformanceCountryCode'] = todo()
mappings['PrimaryPlaceOfPerformanceCountryName'] = copy_subaward_field('principle_place_country')
mappings['Vendor Doing As Business Name'] = copy_subaward_field('dba_name')
mappings['PrimeAwardReportID'] = todo()   # contract_number, fain
mappings['ParentAwardId'] = todo()
mappings['AwardReportMonth'] = todo()
mappings['AwardReportYear'] = todo()
mappings['RecModelQuestion1'] = todo()    # conversion from bool
mappings['RecModelQuestion2'] = todo()    # conversion from bool
mappings['SubawardNumber'] = todo()
mappings['SubawardeeBusinessType'] = todo()
mappings['AwardeeOrRecipientUniqueIdentifier'] = copy_subaward_field(
    'duns')     # check


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
        for key, mapper in mappings.items():
            result[key] = mapper.subcontract(proc, sub)
        yield result

    query = sess.query(FSRSGrant, FSRSSubgrant).\
        filter(FSRSGrant.id == FSRSSubgrant.parent_id).\
        filter(FSRSGrant.fain.in_(fains))
    for grant, sub in query:
        result = OrderedDict()
        for key, mapper in mappings.items():
            result[key] = mapper.subgrant(grant, sub)
        yield result
