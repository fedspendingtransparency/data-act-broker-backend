import logging
from collections import OrderedDict
import copy

from datetime import datetime
from sqlalchemy import case, func

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import get_time_period
from dataactcore.models.domainModels import CGAC, FREC
from dataactcore.models.errorModels import CertifiedErrorMetadata, ErrorMetadata
from dataactcore.models.lookups import (PUBLISH_STATUS_DICT, RULE_SEVERITY_DICT, FILE_TYPE_DICT_LETTER_ID,
                                        FILE_TYPE_DICT_LETTER)
from dataactcore.models.jobModels import Submission, Job, QuarterlyRevalidationThreshold
from dataactcore.models.userModel import User
from dataactcore.models.validationModels import RuleSql, RuleSetting, RuleImpact

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactbroker.helpers.generic_helper import fy
from dataactbroker.helpers.filters_helper import permissions_filter, agency_filter, file_filter
from dataactbroker.helpers.dashboard_helper import FILE_TYPES, agency_has_settings, generate_file_type
from dataactbroker.handlers.agency_handler import get_accessible_agencies


logger = logging.getLogger(__name__)


def list_rule_labels(files, error_level='warning', fabs=False):
    """ Returns a list of rule labels based on the files and error type provided

        Args:
            files: A list of files for which to return rule labels. If blank, return all matching other arguments
            error_level: A string indicating whether to return errors, warnings, or both. Defaults to warning
            fabs: A boolean indicating whether to return FABS or DABS rules. Defaults to False

        Returns:
            JsonResponse of the rule labels the arguments indicate. JsonResponse error if invalid file types are
            provided or any file types are provided for FABS
    """
    # Make sure list is empty when requesting FABS rules
    if fabs and len(files) > 0:
        return JsonResponse.error(ValueError('Files list must be empty for FABS rules'), StatusCode.CLIENT_ERROR)

    invalid_files = [invalid_file for invalid_file in files if invalid_file not in FILE_TYPES]
    if invalid_files:
        return JsonResponse.error(ValueError('The following are not valid file types: {}'.
                                             format(', '.join(invalid_files))),
                                  StatusCode.CLIENT_ERROR)

    sess = GlobalDB.db().session

    rule_label_query = sess.query(RuleSql.rule_label)

    # If the error level isn't "mixed" add a filter on which severity to pull
    if error_level == 'error':
        rule_label_query = rule_label_query.filter_by(rule_severity_id=RULE_SEVERITY_DICT['fatal'])
    elif error_level == 'warning':
        rule_label_query = rule_label_query.filter_by(rule_severity_id=RULE_SEVERITY_DICT['warning'])

    # If specific files have been specified, add a filter to get them
    if files:
        rule_label_query = file_filter(rule_label_query, RuleSql, files)
    elif not fabs:
        # If not the rules are not FABS, exclude FABS rules
        rule_label_query = rule_label_query.filter(RuleSql.file_id != FILE_TYPE_DICT_LETTER_ID['FABS'])
    else:
        # If the rule is FABS, add a filter to only get FABS rules
        rule_label_query = rule_label_query.filter_by(file_id=FILE_TYPE_DICT_LETTER_ID['FABS'])

    return JsonResponse.create(StatusCode.OK, {'labels': [label.rule_label for label in rule_label_query.all()]})


def validate_historic_dashboard_filters(filters, graphs=False):
    """ Validate historic dashboard filters

        Args:
            filters: dictionary representing the filters provided to the historic dashboard endpoints
            graphs: whether or not to validate the files and rules as well

        Exceptions:
            ResponseException if filter is invalid
    """
    required_filters = ['quarters', 'fys', 'agencies']
    if graphs:
        required_filters.extend(['files', 'rules'])
    missing_filters = [required_filter for required_filter in required_filters if required_filter not in filters]
    if missing_filters:
        raise ResponseException('The following filters were not provided: {}'.format(', '.join(missing_filters)),
                                status=StatusCode.CLIENT_ERROR)

    wrong_filter_types = [key for key, value in filters.items() if not isinstance(value, list)]
    if wrong_filter_types:
        raise ResponseException('The following filters were not lists: {}'.format(', '.join(wrong_filter_types)),
                                status=StatusCode.CLIENT_ERROR)

    for quarter in filters['quarters']:
        if quarter not in range(1, 5):
            raise ResponseException('Quarters must be a list of integers, each ranging 1-4, or an empty list.',
                                    status=StatusCode.CLIENT_ERROR)

    current_fy = fy(datetime.now())
    for fiscal_year in filters['fys']:
        if fiscal_year not in range(2017, current_fy + 1):
            raise ResponseException('Fiscal Years must be a list of integers, each ranging from 2017 through the'
                                    ' current fiscal year, or an empty list.', status=StatusCode.CLIENT_ERROR)

    for agency in filters['agencies']:
        if not isinstance(agency, str):
            raise ResponseException('Agencies must be a list of strings, or an empty list.',
                                    status=StatusCode.CLIENT_ERROR)

    if graphs:
        for file_type in filters['files']:
            if file_type not in FILE_TYPES:
                raise ResponseException('Files must be a list of one or more of the following, or an empty list: {}'.
                                        format(', '.join(FILE_TYPES)), status=StatusCode.CLIENT_ERROR)

        for rule in filters['rules']:
            if not isinstance(rule, str):
                raise ResponseException('Rules must be a list of strings, or an empty list.',
                                        status=StatusCode.CLIENT_ERROR)


def validate_table_properties(page, limit, order, sort, sort_options):
    """ Validate table properties like page, limit, and sort

        Args:
            page: page number to use in getting the list
            limit: the number of entries per page
            order: order ascending or descending
            sort: the column to order on
            sort_options: the list of valid options for sorting

        Exceptions:
            ResponseException if filter is invalid
    """

    if not isinstance(page, int) or page <= 0:
        raise ResponseException('Page must be an integer greater than 0', status=StatusCode.CLIENT_ERROR)

    if not isinstance(limit, int) or limit <= 0:
        raise ResponseException('Limit must be an integer greater than 0', status=StatusCode.CLIENT_ERROR)

    if order not in ['asc', 'desc']:
        raise ResponseException('Order must be "asc" or "desc"', status=StatusCode.CLIENT_ERROR)

    if sort not in sort_options:
        raise ResponseException('Sort must be one of: {}'.format(', '.join(sort_options)),
                                status=StatusCode.CLIENT_ERROR)


def apply_historic_dabs_filters(sess, query, filters):
    """ Apply the filters provided to the query provided

        Args:
            sess: the database connection
            query: the baseline sqlalchemy query to work from
            filters: dictionary representing the filters provided to the historic dashboard endpoints

        Exceptions:
            ResponseException if filter is invalid

        Returns:
            the original query with the appropriate filters
    """

    # Applying general user permissions standard for all the filters
    query = permissions_filter(query)

    if filters['quarters']:
        periods = [quarter * 3 for quarter in filters['quarters']]
        query = query.filter(Submission.reporting_fiscal_period.in_(periods))

    if filters['fys']:
        query = query.filter(Submission.reporting_fiscal_year.in_(filters['fys']))

    if filters['agencies']:
        query = agency_filter(sess, query, Submission, Submission, filters['agencies'])

    return query


def apply_historic_dabs_details_filters(query, filters):
    """ Apply the detailed filters provided to the query provided

        Args:
            query: the baseline sqlalchemy query to work from
            filters: dictionary representing the detailed filters provided to the historic dashboard endpoints

        Returns:
            the original query with the appropriate filters
    """

    if filters['files']:
        query = file_filter(query, CertifiedErrorMetadata, filters['files'])

    if filters['rules']:
        query = query.filter(CertifiedErrorMetadata.original_rule_label.in_(filters['rules']))

    return query


def historic_dabs_warning_summary(filters):
    """ Generate a list of submission summaries appropriate on the filters provided

        Args:
            filters: dictionary representing the filters provided to the historic dashboard endpoints

        Return:
            JsonResponse of the submission summaries appropriate on the filters provided
    """
    sess = GlobalDB.db().session

    validate_historic_dashboard_filters(filters, graphs=False)

    summary_query = sess.query(
        Submission.submission_id,
        (Submission.reporting_fiscal_period / 3).label('quarter'),
        Submission.reporting_fiscal_year.label('fy'),
        User.name.label('certifier'),
        case([
            (FREC.frec_code.isnot(None), FREC.frec_code),
            (CGAC.cgac_code.isnot(None), CGAC.cgac_code)
        ]).label('agency_code'),
        case([
            (FREC.agency_name.isnot(None), FREC.agency_name),
            (CGAC.agency_name.isnot(None), CGAC.agency_name)
        ]).label('agency_name')
    ).join(User, User.user_id == Submission.certifying_user_id).\
        outerjoin(CGAC, CGAC.cgac_code == Submission.cgac_code).\
        outerjoin(FREC, FREC.frec_code == Submission.frec_code).\
        filter(Submission.publish_status_id.in_([PUBLISH_STATUS_DICT['published'], PUBLISH_STATUS_DICT['updated']])).\
        filter(Submission.d2_submission.is_(False))

    summary_query = apply_historic_dabs_filters(sess, summary_query, filters)

    # ordering submissions so they all come out in the same order
    summary_query = summary_query.order_by(Submission.reporting_end_date)

    # Build list of codes user has access to, use it to fill in the blanks if no subs are found for some agencies
    user_agencies = get_accessible_agencies()
    perms = {agency['cgac_code']: agency['agency_name'] for agency in user_agencies['cgac_agency_list']}
    perms.update({agency['frec_code']: agency['agency_name'] for agency in user_agencies['frec_agency_list']})
    perm_codes = list(perms.keys())
    # Only care about the codes the user has access to and have requested
    if perm_codes and filters['agencies']:
        perm_codes = list(set(perm_codes) & set(filters['agencies']))

    # Populate submission lists
    results = {}
    for query_result in summary_query.all():
        result_dict = {
            'submission_id': query_result.submission_id,
            'fy': query_result.fy,
            'quarter': query_result.quarter,
            'certifier': query_result.certifier
        }
        if query_result.agency_name in results:
            results[query_result.agency_name].append(result_dict)
        else:
            results[query_result.agency_name] = [result_dict]

    # Fill in the blanks
    for perm in perm_codes:
        if perms[perm] not in results.keys():
            results[perms[perm]] = []

    # Convert to list for ease of use by frontend
    response = [{'agency_name': agency_name, 'submissions': submissions}
                for agency_name, submissions in results.items()]

    return JsonResponse.create(StatusCode.OK, response)


def historic_dabs_warning_graphs(filters):
    """ Generate a list of submission graphs appropriate on the filters provided

        Args:
            filters: dictionary representing the filters provided to the historic dashboard endpoints

        Return:
            JsonResponse of the submission summaries appropriate on the filters provided
    """
    sess = GlobalDB.db().session

    validate_historic_dashboard_filters(filters, graphs=True)

    subs_query = sess.query(
        Submission.submission_id,
        (Submission.reporting_fiscal_period / 3).label('quarter'),
        Submission.reporting_fiscal_year.label('fy'),
        case([
            (FREC.frec_code.isnot(None), FREC.frec_code),
            (CGAC.cgac_code.isnot(None), CGAC.cgac_code)
        ]).label('agency_code'),
        case([
            (FREC.agency_name.isnot(None), FREC.agency_name),
            (CGAC.agency_name.isnot(None), CGAC.agency_name)
        ]).label('agency_name')
    ).outerjoin(CGAC, CGAC.cgac_code == Submission.cgac_code).\
        outerjoin(FREC, FREC.frec_code == Submission.frec_code).\
        filter(Submission.publish_status_id.in_([PUBLISH_STATUS_DICT['published'], PUBLISH_STATUS_DICT['updated']])).\
        filter(Submission.d2_submission.is_(False)).order_by(Submission.submission_id)

    subs_query = apply_historic_dabs_filters(sess, subs_query, filters)

    # get the submission metadata
    sub_metadata = OrderedDict()
    for query_result in subs_query.all():
        sub_id = query_result.submission_id
        sub_metadata[sub_id] = {
            'submission_id': sub_id,
            'fy': query_result.fy,
            'quarter': query_result.quarter,
            'agency': {
                'name': query_result.agency_name,
                'code': query_result.agency_code,
            },
            'total_warnings': 0,
            'warnings': []
        }
    sub_ids = list(sub_metadata.keys())

    # build baseline results dict
    results_data = OrderedDict()
    resulting_files = filters['files'] or FILE_TYPES
    for resulting_file in sorted(resulting_files):
        results_data[resulting_file] = copy.deepcopy(sub_metadata)

    if sub_ids:
        # get metadata for subs/files
        error_metadata_query = sess.query(
            Job.submission_id,
            CertifiedErrorMetadata.file_type_id,
            CertifiedErrorMetadata.target_file_type_id,
            CertifiedErrorMetadata.original_rule_label.label('label'),
            CertifiedErrorMetadata.occurrences.label('instances')
        ).join(CertifiedErrorMetadata, CertifiedErrorMetadata.job_id == Job.job_id).\
            filter(Job.submission_id.in_(sub_ids))

        error_metadata_query = apply_historic_dabs_details_filters(error_metadata_query, filters)

        # ordering warnings so they all come out in the same order
        error_metadata_query = error_metadata_query.order_by(CertifiedErrorMetadata.original_rule_label)

        # Add warnings objects to results dict
        for query_result in error_metadata_query.all():
            file_type = generate_file_type(query_result.file_type_id, query_result.target_file_type_id)
            submission_id = query_result.submission_id

            # update based on warning data
            results_data[file_type][submission_id]['total_warnings'] += query_result.instances
            warning = {
                'label': query_result.label,
                'instances': query_result.instances,
                'percent_total': 0
            }
            results_data[file_type][submission_id]['warnings'].append(warning)

        # Calculate the percentages
        for _, file_dict in results_data.items():
            for _, sub_dict in file_dict.items():
                for warning in sub_dict['warnings']:
                    warning['percent_total'] = round((warning['instances']/sub_dict['total_warnings'])*100)

    # Convert submissions dicts to lists
    results = OrderedDict()
    for file_type, file_dict in results_data.items():
        results[file_type] = [sub_dict for sub_id, sub_dict in file_dict.items()]

    return JsonResponse.create(StatusCode.OK, results)


def historic_dabs_warning_table(filters, page, limit, sort='period', order='desc'):
    """ Returns a list of warnings containing all the information needed for the DABS dashboard warning table based
        on the filters provided that represent one page of the table.

        Args:
            filters: a dict containing the filters provided by the user
            page: page number to use in getting the list
            limit: the number of entries per page
            sort: the column to order on
            order: order ascending or descending

        Returns:
            Limited list of warning metadata and the total number of error metadata entries for the given filters
    """

    # Determine what to order by, default to "period"
    options = {
        'period': {'model': None, 'col': 'fy'},
        'rule_label': {'model': CertifiedErrorMetadata, 'col': 'original_rule_label'},
        'instances': {'model': CertifiedErrorMetadata, 'col': 'occurrences'},
        'description': {'model': CertifiedErrorMetadata, 'col': 'rule_failed'},
        'file': {'model': Job, 'col': 'original_filename'}
    }

    validate_historic_dashboard_filters(filters, graphs=True)
    validate_table_properties(page, limit, order, sort, sort_options=options.keys())

    sess = GlobalDB.db().session

    # Making a query to get all the filenames
    sub_files = sess.query(
        Submission.submission_id,
        (Submission.reporting_fiscal_period / 3).label('quarter'),
        Submission.reporting_fiscal_year.label('fy'),
        func.max(case([(Job.file_type_id == FILE_TYPE_DICT_LETTER_ID['A'],
                        Job.original_filename)])).label('file_A_name'),
        func.max(case([(Job.file_type_id == FILE_TYPE_DICT_LETTER_ID['B'],
                        Job.original_filename)])).label('file_B_name'),
        func.max(case([(Job.file_type_id == FILE_TYPE_DICT_LETTER_ID['C'],
                        Job.original_filename)])).label('file_C_name'),
        func.max(case([(Job.file_type_id == FILE_TYPE_DICT_LETTER_ID['D1'],
                        Job.original_filename)])).label('file_D1_name'),
        func.max(case([(Job.file_type_id == FILE_TYPE_DICT_LETTER_ID['D2'],
                        Job.original_filename)])).label('file_D2_name')
    ).join(Job, Job.submission_id == Submission.submission_id).\
        filter(Submission.publish_status_id.in_([PUBLISH_STATUS_DICT['published'], PUBLISH_STATUS_DICT['updated']])).\
        filter(Submission.d2_submission.is_(False))

    # Apply the basic filters to the cte
    sub_files = apply_historic_dabs_filters(sess, sub_files, filters)

    # Make the query a cte and add a grouping
    sub_files = sub_files.group_by(Submission.submission_id, Submission.reporting_fiscal_period,
                                   Submission.reporting_fiscal_year).cte('sub_files')

    # Base query
    table_query = sess.query(
        sub_files.c.submission_id,
        sub_files.c.quarter,
        sub_files.c.fy,
        sub_files.c.file_A_name,
        sub_files.c.file_B_name,
        sub_files.c.file_C_name,
        sub_files.c.file_D1_name,
        sub_files.c.file_D2_name,
        Job.original_filename,
        Job.file_type_id.label('job_file_type'),
        CertifiedErrorMetadata.original_rule_label,
        CertifiedErrorMetadata.occurrences,
        CertifiedErrorMetadata.rule_failed,
        CertifiedErrorMetadata.file_type_id.label('error_file_type'),
        CertifiedErrorMetadata.target_file_type_id
    ).join(Job, Job.submission_id == sub_files.c.submission_id).\
        join(CertifiedErrorMetadata, CertifiedErrorMetadata.job_id == Job.job_id)

    # Apply filters
    table_query = apply_historic_dabs_details_filters(table_query, filters)

    # Determine how to sort agencies with period
    if sort == 'period':
        sort_order = [sub_files.c.fy, sub_files.c.quarter, CertifiedErrorMetadata.original_rule_label]
    else:
        sort_order = [getattr(options[sort]['model'], options[sort]['col'])]

    # add secondary/tertiary sorts
    if sort in ['file', 'instances']:
        sort_order.append(CertifiedErrorMetadata.rule_failed)
    if sort in ['rule_label', 'description', 'instances']:
        sort_order.extend([sub_files.c.fy, sub_files.c.quarter])

    # Set the sort order
    if order == 'desc':
        sort_order = [order.desc() for order in sort_order]

    table_query = table_query.order_by(*sort_order)

    # Total number of entries in the table
    total_metadata = table_query.count()

    # The page we're on
    offset = limit * (page - 1)
    table_query = table_query.slice(offset, offset + limit)

    response = {'results': [],
                'page_metadata': {
                    'total': total_metadata,
                    'page': page,
                    'limit': limit
                }}

    # Loop through all responses
    for error_metadata in table_query.all():
        # Basic data that's gathered the same way for all entries
        data = {
            'submission_id': error_metadata.submission_id,
            'files': [],
            'fy': error_metadata.fy,
            'quarter': error_metadata.quarter,
            'rule_label': error_metadata.original_rule_label,
            'instance_count': error_metadata.occurrences,
            'rule_description': error_metadata.rule_failed
        }
        # If target file type ID null, that means it's a single-file validation and the original filename can be
        # gathered straight from the job
        if error_metadata.target_file_type_id is None:
            file_type = FILE_TYPE_DICT_LETTER[error_metadata.job_file_type]
            data['files'].append({
                'type': file_type,
                'filename': error_metadata.original_filename
            })
        else:
            # If there's a target file type ID, it's a cross-file and we have to append 2 files to the error metadata
            file_type = FILE_TYPE_DICT_LETTER[error_metadata.error_file_type]
            target_file_type = FILE_TYPE_DICT_LETTER[error_metadata.target_file_type_id]
            data['files'].append({
                'type': file_type,
                'filename': getattr(error_metadata, 'file_{}_name'.format(file_type))
            })
            data['files'].append({
                'type': target_file_type,
                'filename': getattr(error_metadata, 'file_{}_name'.format(target_file_type))
            })
        response['results'].append(data)

    return JsonResponse.create(StatusCode.OK, response)


def active_submission_overview(submission, file, error_level):
    """ Gathers information for the overview section of the active DABS dashboard.

        Args:
            submission: submission to get the overview for
            file: The type of file to get the overview data for
            error_level: whether to get warning or error counts for the overview (possible: warning, error, mixed)

        Returns:
            A response containing overview information of the provided submission for the active DABS dashboard.

        Raises:
            ResponseException if submission provided is a FABS submission.
    """
    if submission.d2_submission:
        raise ResponseException('Submission must be a DABS submission.', status=StatusCode.CLIENT_ERROR)

    # Basic data that can be gathered from just the submission and passed filters
    response = {
        'submission_id': submission.submission_id,
        'duration': 'Quarterly' if submission.is_quarter_format else 'Monthly',
        'reporting_period': get_time_period(submission),
        'certification_deadline': 'N/A',
        'days_remaining': 'N/A',
        'number_of_rules': 0,
        'total_instances': 0
    }

    # File type
    if file in ['A', 'B', 'C']:
        response['file'] = 'File ' + file
    else:
        response['file'] = 'Cross: ' + file.split('-')[1]

    sess = GlobalDB.db().session

    # Agency-specific data
    if submission.frec_code:
        agency = sess.query(FREC).filter_by(frec_code=submission.frec_code).one()
    else:
        agency = sess.query(CGAC).filter_by(cgac_code=submission.cgac_code).one()

    response['agency_name'] = agency.agency_name
    response['icon_name'] = agency.icon_name

    # Deadline information, updates the default values of N/A only if it's a quarter format and the deadline exists
    if submission.is_quarter_format:
        deadline = sess.query(QuarterlyRevalidationThreshold.window_end).\
            filter_by(year=submission.reporting_fiscal_year, quarter=submission.reporting_fiscal_period // 3).first()
        if deadline:
            deadline = deadline.window_end.date()
            today = datetime.now().date()
            if today > deadline:
                response['certification_deadline'] = 'Past Due'
            elif today == deadline:
                response['certification_deadline'] = deadline.strftime('%B %-d, %Y')
                response['days_remaining'] = 'Due Today'
            else:
                response['certification_deadline'] = deadline.strftime('%B %-d, %Y')
                response['days_remaining'] = (deadline - today).days

    # Getting rule counts
    rule_query = sess.query(func.sum(ErrorMetadata.occurrences).label('total_instances'),
                            func.count(1).label('number_of_rules')).\
        join(Job, Job.job_id == ErrorMetadata.job_id).filter(Job.submission_id == submission.submission_id).\
        group_by(ErrorMetadata.job_id)

    # If the error level isn't "mixed" add a filter on which severity to pull
    if error_level == 'error':
        rule_query = rule_query.filter(ErrorMetadata.severity_id == RULE_SEVERITY_DICT['fatal'])
    elif error_level == 'warning':
        rule_query = rule_query.filter(ErrorMetadata.severity_id == RULE_SEVERITY_DICT['warning'])

    rule_query = file_filter(rule_query, ErrorMetadata, [file])

    rule_values = rule_query.first()

    if rule_values:
        response['number_of_rules'] = rule_values.number_of_rules
        response['total_instances'] = rule_values.total_instances

    return JsonResponse.create(StatusCode.OK, response)


def active_submission_table(submission, file, error_level, page=1, limit=5, sort='significance', order='desc'):
    """ Gather a list of warnings/errors based on the filters provided to display in the active dashboard table.

        Args:
            submission: submission to get the table data for
            file: The type of file to get the table data for
            error_level: whether to get warnings, errors, or both for the table (possible: warning, error, mixed)
            page: page number to use in getting the list
            limit: the number of entries per page
            sort: the column to order on
            order: order ascending or descending

        Returns:
            A response containing a list of results for the active submission dashboard table and the metadata for
            the table.

        Raises:
            ResponseException if submission provided is a FABS submission.
    """
    if submission.d2_submission:
        raise ResponseException('Submission must be a DABS submission.', status=StatusCode.CLIENT_ERROR)

    # Basic information that is provided by the user and defaults for the rest
    response = {
        'page_metadata': {
            'total': 0,
            'page': page,
            'limit': limit,
            'submission_id': submission.submission_id,
            'files': []
        },
        'results': []
    }

    # File type
    if file in ['A', 'B', 'C']:
        response['page_metadata']['files'] = [file]
    else:
        letters = file.split('-')[1]
        response['page_metadata']['files'] = [letters[:1], letters[1:]]

    sess = GlobalDB.db().session

    agency_code = submission.frec_code or submission.cgac_code
    has_settings = agency_has_settings(sess, agency_code, file)

    # Initial query
    table_query = sess.query(ErrorMetadata.original_rule_label, ErrorMetadata.occurrences, ErrorMetadata.rule_failed,
                             RuleSql.category, RuleSetting.priority, RuleImpact.name.label('impact_name')).\
        join(Job, Job.job_id == ErrorMetadata.job_id).\
        join(RuleSql, RuleSql.rule_label == ErrorMetadata.original_rule_label).\
        join(RuleSetting, RuleSetting.rule_id == RuleSql.rule_sql_id).\
        join(RuleImpact, RuleImpact.rule_impact_id == RuleSetting.impact_id).\
        filter(Job.submission_id == submission.submission_id)

    # Determining which settings to use
    if has_settings:
        table_query = table_query.filter(RuleSetting.agency_code == agency_code)
    else:
        table_query = table_query.filter(RuleSetting.agency_code.is_(None))

    # If the error level isn't "mixed" add a filter on which severity to pull
    if error_level == 'error':
        table_query = table_query.filter(ErrorMetadata.severity_id == RULE_SEVERITY_DICT['fatal'])
    elif error_level == 'warning':
        table_query = table_query.filter(ErrorMetadata.severity_id == RULE_SEVERITY_DICT['warning'])

    table_query = file_filter(table_query, ErrorMetadata, [file])

    # Total number of entries in the table
    response['page_metadata']['total'] = table_query.count()

    # Determine what to order by, default to "significance"
    options = {
        'significance': {'model': RuleSetting, 'col': 'priority'},
        'rule_label': {'model': ErrorMetadata, 'col': 'original_rule_label'},
        'instances': {'model': ErrorMetadata, 'col': 'occurrences'},
        'category': {'model': RuleSql, 'col': 'category'},
        'impact': {'model': RuleSetting, 'col': 'impact_id'},
        'description': {'model': ErrorMetadata, 'col': 'rule_failed'}
    }

    sort_order = [getattr(options[sort]['model'], options[sort]['col'])]

    # add secondary sorts
    if sort in ['instances', 'category', 'impact']:
        sort_order.append(RuleSetting.priority)

    # Set the sort order
    if order == 'desc':
        sort_order = [order.desc() for order in sort_order]

    table_query = table_query.order_by(*sort_order)

    # The page we're on
    offset = limit * (page - 1)
    table_query = table_query.slice(offset, offset + limit)

    for result in table_query.all():
        response['results'].append({
            'significance': result.priority,
            'rule_label': result.original_rule_label,
            'instance_count': result.occurrences,
            'category': result.category,
            'impact': result.impact_name,
            'rule_description': result.rule_failed
        })

    return JsonResponse.create(StatusCode.OK, response)
