from sqlalchemy import or_, and_, func
from sqlalchemy.orm import outerjoin
from flask import g
from dataactcore.models.lookups import FILE_TYPE_DICT_LETTER_ID, RULE_SEVERITY_DICT

from dataactcore.models.domainModels import CGAC, FREC
from dataactcore.models.errorModels import PublishedErrorMetadata, ErrorMetadata
from dataactcore.models.jobModels import Submission
from dataactcore.models.validationModels import RuleSql, RuleSetting
from dataactcore.utils.ResponseError import ResponseError
from dataactcore.utils.statusCode import StatusCode


def agency_filter(sess, query, cgac_model, frec_model, agency_list):
    """ Given the provided query, add a filter by agencies provided the agency list. Note that this does not include
        the additional permissions filter listed in this file.

        Arguments:
            sess: the database connection
            query: the sqlalchemy query to apply the filters to
            cgac_model: the model to apply the cgacs filter
            frec_model: the model to apply the frecs filter
            agency_list: list of strings representing the agency codes to filter with

        Raises:
            ResponseError: if any of the strings in the agency_list are invalid

        Returns:
            the same queryset provided with agency filters included
    """
    agency_filters = []

    cgac_codes = [cgac_code for cgac_code in agency_list if isinstance(cgac_code, str) and len(cgac_code) == 3]
    frec_codes = [frec_code for frec_code in agency_list if isinstance(frec_code, str) and len(frec_code) == 4]

    if len(cgac_codes) + len(frec_codes) != len(agency_list):
        raise ResponseError('All codes in the agency_codes filter must be valid agency codes',
                            StatusCode.CLIENT_ERROR)
    # If the number of CGACs or FRECs returned from a query using the codes doesn't match the length of
    # each list (ignoring duplicates) then something included wasn't a valid agency
    cgac_list = set(cgac_codes)
    frec_list = set(frec_codes)
    if (cgac_list and sess.query(CGAC).filter(CGAC.cgac_code.in_(cgac_list)).count() != len(cgac_list)) or \
            (frec_list and sess.query(FREC).filter(FREC.frec_code.in_(frec_list)).count() != len(frec_list)):
        raise ResponseError("All codes in the agency_codes filter must be valid agency codes",
                            StatusCode.CLIENT_ERROR)
    if len(cgac_list) > 0:
        agency_filters.append(cgac_model.cgac_code.in_(cgac_list))
    if len(frec_list) > 0:
        agency_filters.append(frec_model.frec_code.in_(frec_list))

    return query.filter(or_(*agency_filters)) if agency_filters else query


def permissions_filter(query):
    """ Given the provided query, add a filter to only include agencies the user has access to.

        Arguments:
            query: the sqlalchemy query to apply the filters to

        Returns:
            the same queryset provided with permissions filter included
    """
    if not g.user.website_admin:
        affiliation_filters = []
        cgac_codes = [aff.cgac.cgac_code for aff in g.user.affiliations if aff.cgac]
        frec_codes = [aff.frec.frec_code for aff in g.user.affiliations if aff.frec]

        affiliation_filters.append(Submission.user_id == g.user.user_id)

        if cgac_codes:
            affiliation_filters.append(Submission.cgac_code.in_(cgac_codes))
        if frec_codes:
            affiliation_filters.append(Submission.frec_code.in_(frec_codes))
        query = query.filter(or_(*affiliation_filters)) if affiliation_filters else query

    return query


def file_filter(query, file_model, files):
    """ Given the provided query, add a filter by files provided the files list.

        Arguments:
            query: the sqlalchemy query to apply the filters to
            file_model: the model to apply the file filter
            files: list of files representing the agency codes to filter with

        Returns:
            the same queryset provided with file filters included
    """
    model_file_type_id = {
        PublishedErrorMetadata: 'file_type_id',
        ErrorMetadata: 'file_type_id',
        RuleSql: 'file_id',
        RuleSetting: 'file_id'
    }
    if file_model not in model_file_type_id:
        valid_file_models = [model_file_type.__name__ for model_file_type in model_file_type_id.keys()]
        error_message = 'Invalid file model. Use one of the following instead: {}.'
        raise ResponseError(error_message.format(', '.join(sorted(valid_file_models))))

    file_type_filters = []
    if files:
        for file_type in files:
            file_id = getattr(file_model, model_file_type_id[file_model])
            target_file_id = getattr(file_model, 'target_{}'.format(model_file_type_id[file_model]))
            if file_type in ['A', 'B', 'C']:
                file_type_filters.append(and_(file_id == FILE_TYPE_DICT_LETTER_ID[file_type],
                                              target_file_id.is_(None)))
            else:
                file_types = file_type.split('-')[1]
                # Append both orders of the source/target files to the list
                file_type_filters.append(and_(file_id == FILE_TYPE_DICT_LETTER_ID[file_types[:1]],
                                              target_file_id == FILE_TYPE_DICT_LETTER_ID[file_types[1:]]))
                file_type_filters.append(and_(file_id == FILE_TYPE_DICT_LETTER_ID[file_types[1:]],
                                              target_file_id == FILE_TYPE_DICT_LETTER_ID[file_types[:1]]))
    return query.filter(or_(*file_type_filters)) if file_type_filters else query


def rule_severity_filter(query, error_level, error_model=ErrorMetadata):
    """ Given the provided query, add a filter by files provided the files list.

        Arguments:
            query: the sqlalchemy query to apply the filters to
            error_level: the error level to filter on (could be 'error' or 'warning')
            error_model: the model to apply the filter to (must have a severity_id field)

        Returns:
            the same queryset provided with rule severity filter included
    """
    # If the error level isn't "mixed" add a filter on which severity to pull
    if error_level == 'error':
        query = query.filter(error_model.severity_id == RULE_SEVERITY_DICT['fatal'])
    elif error_level == 'warning':
        query = query.filter(error_model.severity_id == RULE_SEVERITY_DICT['warning'])

    return query


def tas_agency_filter(sess, agency_code, filter_model):
    """ Adds a filter by agency using TAS bucketing logic to the provided query

        Args:
            sess: database session
            agency_code: the agency code to filter by
            filter_model: the model to filter on

        Return:
            An array of agency filters that can be used in a query
    """
    # set a boolean to determine if the original agency code is frec or cgac
    frec_provided = len(agency_code) == 4

    # Make a list of FRECs to compare to for 011 AID entries
    frec_list = []
    if not frec_provided:
        frec_list = sess.query(FREC.frec_code).select_from(outerjoin(CGAC, FREC, CGAC.cgac_id == FREC.cgac_id)). \
            filter(CGAC.cgac_code == agency_code).all()
        # Put the frec list in a format that can be read by a filter
        frec_list = [frec.frec_code for frec in frec_list]

    # Group agencies together that need to be grouped
    # NOTE: If these change, update A33.1 to match
    agency_array = []
    if agency_code == '097':
        agency_array = ['017', '021', '057', '097']
    elif agency_code == '020':
        agency_array = ['020', '580', '373']
    elif agency_code == '077':
        agency_array = ['077', '071']
    elif agency_code == '089':
        agency_array = ['089', '486']
    elif agency_code == '1601':
        agency_array = ['1601', '016']
    elif agency_code == '1125':
        agency_array = ['1125', '011']
    elif agency_code == '1100':
        agency_array = ['1100', '256']

    # Save the ATA filter
    agency_filters = []
    if not agency_array:
        agency_filters.append(filter_model.allocation_transfer_agency == agency_code)
    else:
        agency_filters.append(filter_model.allocation_transfer_agency.in_(agency_array))

    # Save the AID filter
    if agency_code in ['097', '020', '077', '089']:
        agency_filters.append(and_(filter_model.allocation_transfer_agency.is_(None),
                                   filter_model.agency_identifier.in_(agency_array)))
    elif agency_code == '1100':
        agency_filters.append(and_(filter_model.allocation_transfer_agency.is_(None),
                                   or_(filter_model.agency_identifier == '256',
                                       filter_model.fr_entity_type == '1100')))
    elif not frec_provided:
        agency_filters.append(and_(filter_model.allocation_transfer_agency.is_(None),
                                   filter_model.agency_identifier == agency_code))
    else:
        agency_filters.append(and_(filter_model.allocation_transfer_agency.is_(None),
                                   filter_model.fr_entity_type == agency_code))

    # If we're checking a CGAC, we want to filter on all the related FRECs for AID 011
    if frec_list:
        agency_filters.append(and_(filter_model.allocation_transfer_agency.is_(None),
                                   filter_model.agency_identifier == '011',
                                   filter_model.fr_entity_type.in_(frec_list)))

    # Filtering for specific TAS exceptions
    tas_exception_filter(agency_filters, agency_code, filter_model, 'add')

    return agency_filters


def tas_exception_filter(query_filters, agency_code, tas_model, filter_mode):
    """ Adds a filter for special exception TAS codes to go into different agency files

            Args:
                query_filters: the array of filters to use in the query or the query itself depending on filter_mode
                agency_code: the agency code to check
                tas_model: the model to filter on
                filter_mode: whether to add or ignore TAS, can be 'ignore' or 'add'

            Return:
                The same queryset provided with an additional filter if relevant if in 'add' mode, no return otherwise
        """
    ignore_list = {'020': ['020-X-5688-000']}
    extra_list = {'070': ['020-X-5688-000']}

    # Ignoring anything that needs ignoring
    if filter_mode == 'ignore':
        if agency_code in ignore_list.keys():
            return query_filters.filter(func.upper(tas_model.display_tas).notin_(ignore_list[agency_code]))
        else:
            return query_filters

    # Adding anything that needs adding
    if filter_mode == 'add' and agency_code in extra_list.keys():
        query_filters.append(func.upper(tas_model.display_tas).in_(extra_list[agency_code]))
