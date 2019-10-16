from sqlalchemy import or_, and_
from flask import g
from dataactcore.models.lookups import FILE_TYPE_DICT_LETTER_ID

from dataactcore.models.domainModels import CGAC, FREC
from dataactcore.models.errorModels import CertifiedErrorMetadata
from dataactcore.models.jobModels import Submission
from dataactcore.models.validationModels import RuleSql
from dataactcore.utils.responseException import ResponseException
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
            ResponseException: if any of the strings in the agency_list are invalid

        Returns:
            the same queryset provided with agency filters included
    """
    agency_filters = []

    cgac_codes = [cgac_code for cgac_code in agency_list if isinstance(cgac_code, str) and len(cgac_code) == 3]
    frec_codes = [frec_code for frec_code in agency_list if isinstance(frec_code, str) and len(frec_code) == 4]

    if len(cgac_codes) + len(frec_codes) != len(agency_list):
        raise ResponseException('All codes in the agency_codes filter must be valid agency codes',
                                StatusCode.CLIENT_ERROR)
    # If the number of CGACs or FRECs returned from a query using the codes doesn't match the length of
    # each list (ignoring duplicates) then something included wasn't a valid agency
    cgac_list = set(cgac_codes)
    frec_list = set(frec_codes)
    if (cgac_list and sess.query(CGAC).filter(CGAC.cgac_code.in_(cgac_list)).count() != len(cgac_list)) or \
            (frec_list and sess.query(FREC).filter(FREC.frec_code.in_(frec_list)).count() != len(frec_list)):
        raise ResponseException("All codes in the agency_codes filter must be valid agency codes",
                                StatusCode.CLIENT_ERROR)
    if len(cgac_list) > 0:
        agency_filters.append(cgac_model.cgac_code.in_(cgac_list))
    if len(frec_list) > 0:
        agency_filters.append(frec_model.frec_code.in_(frec_list))

    return query.filter(or_(*agency_filters))


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
        query = query.filter(or_(*affiliation_filters))

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
        CertifiedErrorMetadata: 'file_type_id',
        RuleSql: 'file_id'
    }
    if file_model not in model_file_type_id:
        valid_file_models = [model_file_type.__name__ for model_file_type in model_file_type_id.keys()]
        error_message = 'Invalid file model. Use one of the following instead: {}.'
        raise ResponseException(error_message.format(', '.join(valid_file_models)))

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
    return query.filter(or_(*file_type_filters))
