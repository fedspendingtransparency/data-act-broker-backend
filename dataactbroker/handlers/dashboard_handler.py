import logging
from collections import OrderedDict
import copy

from datetime import datetime
from sqlalchemy import case, func, and_

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import get_time_period, get_certification_deadline
from dataactcore.models.domainModels import CGAC, FREC, is_not_distinct_from
from dataactcore.models.errorModels import PublishedErrorMetadata, ErrorMetadata
from dataactcore.models.lookups import (PUBLISH_STATUS_DICT, RULE_SEVERITY_DICT, FILE_TYPE_DICT_LETTER_ID,
                                        FILE_TYPE_DICT_LETTER, RULE_IMPACT_DICT_ID)
from dataactcore.models.jobModels import Submission, Job
from dataactcore.models.userModel import User
from dataactcore.models.validationModels import RuleSql, RuleSetting, RuleImpact

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactbroker.helpers.generic_helper import fy
from dataactbroker.helpers.filters_helper import permissions_filter, agency_filter, file_filter, rule_severity_filter
from dataactbroker.helpers.dashboard_helper import FILE_TYPES, agency_settings_filter, generate_file_type


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
    required_filters = ['periods', 'fys', 'agencies']
    if graphs:
        required_filters.extend(['files', 'rules'])
    missing_filters = [required_filter for required_filter in required_filters if required_filter not in filters]
    if missing_filters:
        raise ResponseException('The following filters were not provided: {}'.format(', '.join(missing_filters)),
                                status=StatusCode.CLIENT_ERROR)

    wrong_filter_types_list = [key for key, value in filters.items() if not isinstance(value, list)]
    if wrong_filter_types_list:
        raise ResponseException('The following filters were not lists: {}'.format(', '.join(wrong_filter_types_list)),
                                status=StatusCode.CLIENT_ERROR)

    for period in filters['periods']:
        if period not in range(2, 13):
            raise ResponseException('Periods must be a list of integers, each ranging 2-12, or an empty list.',
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

    if filters['periods']:
        query = query.filter(Submission.reporting_fiscal_period.in_(filters['periods']))

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
        query = file_filter(query, PublishedErrorMetadata, filters['files'])

    if filters['rules']:
        query = query.filter(PublishedErrorMetadata.original_rule_label.in_(filters['rules']))

    return query


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
        Submission.reporting_fiscal_period.label('period'),
        Submission.reporting_fiscal_year.label('fy'),
        Submission.is_quarter_format.label('is_quarter'),
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
            'period': query_result.period,
            'is_quarter': query_result.is_quarter,
            'agency': {
                'name': query_result.agency_name,
                'code': query_result.agency_code,
            },
            'total_warnings': 0,
            'filtered_warnings': 0,
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
            PublishedErrorMetadata.file_type_id,
            PublishedErrorMetadata.target_file_type_id,
            PublishedErrorMetadata.original_rule_label.label('label'),
            PublishedErrorMetadata.occurrences.label('instances')
        ).join(PublishedErrorMetadata, PublishedErrorMetadata.job_id == Job.job_id).\
            filter(Job.submission_id.in_(sub_ids))

        # Get the total number of warnings for each submission
        total_warnings_query = sess.query(
            func.coalesce(func.sum(PublishedErrorMetadata.occurrences), 0).label('total_instances'),
            PublishedErrorMetadata.file_type_id,
            PublishedErrorMetadata.target_file_type_id,
            Job.submission_id
        ).join(Job, Job.job_id == PublishedErrorMetadata.job_id).filter(Job.submission_id.in_(sub_ids)).\
            group_by(Job.submission_id, PublishedErrorMetadata.file_type_id, PublishedErrorMetadata.target_file_type_id)

        error_metadata_query = apply_historic_dabs_details_filters(error_metadata_query, filters)
        total_warnings_query = file_filter(total_warnings_query, PublishedErrorMetadata, filters['files'])

        # ordering warnings so they all come out in the same order
        error_metadata_query = error_metadata_query.order_by(PublishedErrorMetadata.original_rule_label)

        # Add warnings objects to results dict
        for query_result in error_metadata_query.all():
            file_type = generate_file_type(query_result.file_type_id, query_result.target_file_type_id)
            submission_id = query_result.submission_id

            # update based on warning data
            results_data[file_type][submission_id]['filtered_warnings'] += query_result.instances
            warning = {
                'label': query_result.label,
                'instances': query_result.instances,
                'percent_total': 0
            }
            results_data[file_type][submission_id]['warnings'].append(warning)

        # Add total warnings to results dict
        for query_result in total_warnings_query.all():
            file_type = generate_file_type(query_result.file_type_id, query_result.target_file_type_id)
            submission_id = query_result.submission_id
            results_data[file_type][submission_id]['total_warnings'] += query_result.total_instances

        # Calculate the percentages
        for _, file_dict in results_data.items():
            for _, sub_dict in file_dict.items():
                for warning in sub_dict['warnings']:
                    warning['percent_total'] = round((warning['instances'] / sub_dict['total_warnings']) * 100)

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
        'period': {'model': Submission, 'col': 'reporting_fiscal_year'},
        'rule_label': {'model': PublishedErrorMetadata, 'col': 'original_rule_label'},
        'instances': {'model': PublishedErrorMetadata, 'col': 'occurrences'},
        'description': {'model': PublishedErrorMetadata, 'col': 'rule_failed'},
        'submission_id': {'model': Submission, 'col': 'submission_id'},
        'submitted_by': {'model': User, 'col': 'name'}
    }

    validate_historic_dashboard_filters(filters, graphs=True)
    validate_table_properties(page, limit, order, sort, sort_options=options.keys())

    sess = GlobalDB.db().session

    # Base query
    table_query = sess.query(
        Submission.submission_id,
        Submission.reporting_fiscal_period,
        Submission.reporting_fiscal_year,
        Submission.is_quarter_format,
        User.name.label('certifier'),
        Job.file_type_id.label('job_file_type'),
        PublishedErrorMetadata.original_rule_label,
        PublishedErrorMetadata.occurrences,
        PublishedErrorMetadata.rule_failed,
        PublishedErrorMetadata.file_type_id.label('error_file_type'),
        PublishedErrorMetadata.target_file_type_id
    ).join(Job, Job.submission_id == Submission.submission_id).\
        join(PublishedErrorMetadata, PublishedErrorMetadata.job_id == Job.job_id).\
        join(User, User.user_id == Submission.publishing_user_id). \
        filter(Submission.publish_status_id.in_([PUBLISH_STATUS_DICT['published'], PUBLISH_STATUS_DICT['updated']])). \
        filter(Submission.d2_submission.is_(False))

    # Apply filters
    table_query = apply_historic_dabs_filters(sess, table_query, filters)
    table_query = apply_historic_dabs_details_filters(table_query, filters)

    # Initial sort for each column
    sort_order = [getattr(options[sort]['model'], options[sort]['col'])]

    # add secondary/tertiary sorts
    if sort == 'period':
        sort_order.append(Submission.reporting_fiscal_period)
    if sort in ['submitted_by', 'rule_label', 'instances', 'description']:
        sort_order.append(Submission.submission_id)
    if sort in ['period', 'instances', 'submission_id', 'submitted_by']:
        sort_order.append(PublishedErrorMetadata.original_rule_label)

    # Set the sort order
    if order == 'desc':
        sort_order = [order.desc() for order in sort_order]

    table_query = table_query.order_by(*sort_order)

    # Total number of entries in the table
    total_metadata = table_query.count()

    # The page we're on
    offset = limit * (page - 1)
    table_query = table_query.slice(offset, offset + limit)

    response = {
        'results': [],
        'page_metadata': {
            'total': total_metadata,
            'page': page,
            'limit': limit
        }
    }

    # Loop through all responses
    for error_metadata in table_query.all():
        # Basic data that's gathered the same way for all entries
        data = {
            'submission_id': error_metadata.submission_id,
            'files': [],
            'fy': error_metadata.reporting_fiscal_year,
            'period': error_metadata.reporting_fiscal_period,
            'is_quarter': error_metadata.is_quarter_format,
            'rule_label': error_metadata.original_rule_label,
            'instance_count': error_metadata.occurrences,
            'rule_description': error_metadata.rule_failed,
            'submitted_by': error_metadata.certifier
        }
        # If target file type ID null, that means it's a single-file validation and the file type can be
        # gathered straight from the job
        if error_metadata.target_file_type_id is None:
            file_type = FILE_TYPE_DICT_LETTER[error_metadata.job_file_type]
            data['files'] = [file_type]
        else:
            # If there's a target file type ID, it's a cross-file and we have to append 2 files to the error metadata
            file_type = FILE_TYPE_DICT_LETTER[error_metadata.error_file_type]
            target_file_type = FILE_TYPE_DICT_LETTER[error_metadata.target_file_type_id]
            data['files'] = [file_type, target_file_type]
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

    # Deadline information, updates the default values of N/A only if it's not a test and the deadline exists
    if not submission.test_submission:
        deadline = get_certification_deadline(submission)
        if deadline:
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

    rule_query = rule_severity_filter(rule_query, error_level, ErrorMetadata)
    rule_query = file_filter(rule_query, ErrorMetadata, [file])

    rule_values = rule_query.first()

    if rule_values:
        response['number_of_rules'] = rule_values.number_of_rules
        response['total_instances'] = rule_values.total_instances

    return JsonResponse.create(StatusCode.OK, response)


def get_impact_counts(submission, file, error_level):
    """ Gathers information for the impact count section of the active DABS dashboard.

            Args:
                submission: submission to get the impact counts for
                file: The type of file to get the impact counts for
                error_level: whether to get warning or error counts for the impact counts (possible: warning, error,
                    mixed)

            Returns:
                A response containing impact count information of the provided submission for the active DABS dashboard.

            Raises:
                ResponseException if submission provided is a FABS submission.
        """
    if submission.d2_submission:
        raise ResponseException('Submission must be a DABS submission.', status=StatusCode.CLIENT_ERROR)

    # Basic data that can be gathered from just the submission and passed filters
    response = {
        'low': {
            'total': 0,
            'rules': []
        },
        'medium': {
            'total': 0,
            'rules': []
        },
        'high': {
            'total': 0,
            'rules': []
        }
    }

    sess = GlobalDB.db().session

    # Initial query
    impact_query = sess.query(ErrorMetadata.original_rule_label, ErrorMetadata.occurrences, ErrorMetadata.rule_failed,
                              RuleSetting.impact_id).\
        join(Job, Job.job_id == ErrorMetadata.job_id). \
        join(RuleSetting, and_(ErrorMetadata.original_rule_label == RuleSetting.rule_label,
                               ErrorMetadata.file_type_id == RuleSetting.file_id,
                               is_not_distinct_from(ErrorMetadata.target_file_type_id, RuleSetting.target_file_id))).\
        filter(Job.submission_id == submission.submission_id)

    agency_code = submission.frec_code or submission.cgac_code
    impact_query = agency_settings_filter(sess, impact_query, agency_code, file)
    impact_query = rule_severity_filter(impact_query, error_level, ErrorMetadata)
    impact_query = file_filter(impact_query, RuleSetting, [file])

    for result in impact_query.all():
        response[RULE_IMPACT_DICT_ID[result.impact_id]]['total'] += 1
        response[RULE_IMPACT_DICT_ID[result.impact_id]]['rules'].append({
            'rule_label': result.original_rule_label,
            'instances': result.occurrences,
            'rule_description': result.rule_failed
        })

    return JsonResponse.create(StatusCode.OK, response)


def get_significance_counts(submission, file, error_level):
    """ Gathers information for the signficances section of the active DABS dashboard.

            Args:
                submission: submission to get the significance counts for
                file: The type of file to get the significance counts for
                error_level: whether to get warning or error counts for the significance counts (possible: warning,
                             error, mixed)

            Returns:
                A response containing significance data of the provided submission for the active DABS dashboard.

            Raises:
                ResponseException if submission provided is a FABS submission.
        """
    if submission.d2_submission:
        raise ResponseException('Submission must be a DABS submission.', status=StatusCode.CLIENT_ERROR)

    # Basic data that can be gathered from just the submission and passed filters
    response = {
        'total_instances': 0,
        'rules': []
    }

    sess = GlobalDB.db().session

    # Initial query
    significance_query = sess.query(ErrorMetadata.original_rule_label, ErrorMetadata.occurrences,
                                    ErrorMetadata.rule_failed, RuleSetting.priority, RuleSql.category,
                                    RuleSetting.impact_id).\
        join(Job, Job.job_id == ErrorMetadata.job_id). \
        join(RuleSql, and_(RuleSql.rule_label == ErrorMetadata.original_rule_label,
                           RuleSql.file_id == ErrorMetadata.file_type_id,
                           is_not_distinct_from(RuleSql.target_file_id, ErrorMetadata.target_file_type_id))).\
        join(RuleSetting, and_(RuleSql.rule_label == RuleSetting.rule_label, RuleSql.file_id == RuleSetting.file_id,
                               is_not_distinct_from(RuleSql.target_file_id, RuleSetting.target_file_id))).\
        filter(Job.submission_id == submission.submission_id)

    agency_code = submission.frec_code or submission.cgac_code
    significance_query = agency_settings_filter(sess, significance_query, agency_code, file)
    significance_query = rule_severity_filter(significance_query, error_level, ErrorMetadata)
    significance_query = file_filter(significance_query, RuleSetting, [file])

    # Ordering by significance to help process the results
    significance_query = significance_query.order_by(RuleSetting.priority)

    for result in significance_query.all():
        response['rules'].append({
            'rule_label': result.original_rule_label,
            'category': result.category,
            'significance': result.priority,
            'impact': RULE_IMPACT_DICT_ID[result.impact_id],
            'instances': result.occurrences
        })
        response['total_instances'] += result.occurrences

    # Calculate the percentages
    for rule_dict in response['rules']:
        rule_dict['percentage'] = round((rule_dict['instances'] / response['total_instances']) * 100, 1)

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

    # Initial query
    table_query = sess.query(ErrorMetadata.original_rule_label, ErrorMetadata.occurrences, ErrorMetadata.rule_failed,
                             RuleSql.category, RuleSetting.priority, RuleImpact.name.label('impact_name')).\
        join(Job, Job.job_id == ErrorMetadata.job_id).\
        join(RuleSql, and_(RuleSql.rule_label == ErrorMetadata.original_rule_label,
                           RuleSql.file_id == ErrorMetadata.file_type_id,
                           is_not_distinct_from(RuleSql.target_file_id, ErrorMetadata.target_file_type_id))).\
        join(RuleSetting, and_(RuleSql.rule_label == RuleSetting.rule_label, RuleSql.file_id == RuleSetting.file_id,
                               is_not_distinct_from(RuleSql.target_file_id, RuleSetting.target_file_id))).\
        join(RuleImpact, RuleImpact.rule_impact_id == RuleSetting.impact_id).\
        filter(Job.submission_id == submission.submission_id)

    agency_code = submission.frec_code or submission.cgac_code
    table_query = agency_settings_filter(sess, table_query, agency_code, file)
    table_query = rule_severity_filter(table_query, error_level, ErrorMetadata)
    table_query = file_filter(table_query, RuleSql, [file])

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
