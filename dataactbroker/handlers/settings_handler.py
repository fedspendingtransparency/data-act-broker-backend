import logging

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactcore.interfaces.db import GlobalDB
from dataactbroker.handlers.dashboard_handler import FILE_TYPES, generate_file_type
from dataactbroker.helpers.filters_helper import file_filter
from dataactcore.models.lookups import RULE_IMPACT_DICT, RULE_SEVERITY_DICT
from dataactcore.models.domainModels import CGAC, FREC
from dataactcore.models.validationModels import RuleSetting, RuleImpact, RuleSql


logger = logging.getLogger(__name__)


def load_default_rule_settings(sess):
    """ Populates the default rule settings to the database

        Args:
            sess: connection to the database
    """
    priorities = {}
    rule_settings = []
    for rule in sess.query(RuleSql).order_by(RuleSql.rule_sql_id).all():
        file_type = generate_file_type(rule.file_id, rule.target_file_id)
        if file_type not in priorities:
            priorities[file_type] = {'error': 1, 'warning': 1}

        if rule.rule_severity_id == RULE_SEVERITY_DICT['warning']:
            rule_settings.append(RuleSetting(rule_id=rule.rule_sql_id, agency_code=None,
                                             priority=priorities[file_type]['warning'],
                                             impact_id=RULE_IMPACT_DICT['high']))
            priorities[file_type]['warning'] += 1
        else:
            rule_settings.append(RuleSetting(rule_id=rule.rule_sql_id, agency_code=None,
                                             priority=priorities[file_type]['error'],
                                             impact_id=RULE_IMPACT_DICT['high']))
            priorities[file_type]['error'] += 1

    sess.add_all(rule_settings)
    sess.commit()


def agency_has_settings(sess, agency_code, file):
    """ Helper function to determine if the agency has saved any settings of this file type

        Args:
            agency_code: the agency code to work with
            file: the rule's file type

        Returns:
            True if the agency has saved their settings for this file type
    """

    # Check to see if agency has saved their settings for this file type
    query = sess.query(RuleSetting).\
        join(RuleSql, RuleSql.rule_sql_id == RuleSetting.rule_id).filter(RuleSetting.agency_code == agency_code)
    query = file_filter(query, RuleSql, [file])
    return (query.count() > 0)


def list_rule_settings(agency_code, file):
    """ Returns a list of prioritized rules an agency.

        Args:
            agency_code: string of the agency's CGAC/FREC code
            file: the rule's file type

        Returns:
            Ordered list of rules prioritized by an agency

        Raises:
            ResponseException if invalid agency code or file type
    """
    sess = GlobalDB.db().session

    if file not in FILE_TYPES:
        raise ResponseException('Invalid file type: {}'.format(file), StatusCode.CLIENT_ERROR)
    if (sess.query(CGAC).filter(CGAC.cgac_code == agency_code).count() == 0) and \
            (sess.query(FREC).filter(FREC.frec_code == agency_code).count() == 0):
        raise ResponseException('Invalid agency_code: {}'.format(agency_code), StatusCode.CLIENT_ERROR)

    # Get the base query with the file filter
    rule_settings_query = sess.query(RuleSetting.priority, RuleSql.rule_label, RuleImpact.name,
                                     RuleSql.rule_error_message, RuleSql.rule_severity_id).\
        join(RuleSql, RuleSql.rule_sql_id == RuleSetting.rule_id).\
        join(RuleImpact, RuleImpact.rule_impact_id == RuleSetting.impact_id)
    rule_settings_query = file_filter(rule_settings_query, RuleSql, [file])

    # Filter settings by agency. If they haven't set theirs, use the defaults.
    if agency_has_settings(sess, agency_code, file):
        agency_filter = (RuleSetting.agency_code == agency_code)
    else:
        agency_filter = RuleSetting.agency_code.is_(None)
    rule_settings_query = rule_settings_query.filter(agency_filter)

    # Order by priority/significance
    rule_settings_query = rule_settings_query.order_by(RuleSetting.priority)

    errors = []
    warnings = []
    for rule in rule_settings_query.all():
        rule_dict = {
            'label': rule.rule_label,
            'description': rule.rule_error_message,
            'significance': rule.priority,
            'impact': rule.name
        }
        if rule.rule_severity_id == RULE_SEVERITY_DICT['warning']:
            warnings.append(rule_dict)
        else:
            errors.append(rule_dict)

    return JsonResponse.create(StatusCode.OK, {'warnings': warnings, 'errors': errors})
