import logging

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactcore.interfaces.db import GlobalDB
from dataactbroker.helpers.filters_helper import file_filter
from dataactbroker.helpers.dashboard_helper import FILE_TYPES, generate_file_type, agency_has_settings
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


def validate_rule_dict(rule_dict, rule_label_mapping):
    """ Given a dictionary representing a rule to save, validate it.

        Args:
            rule_dict: the rule dict provided
            rule_label_mapping: dict of available rule labels to rule ids

        Raises:
            ResponseException if rule dict is invalid
    """
    rule_dict_keys = {'label', 'impact'}
    if not rule_dict_keys <= set(rule_dict.keys()):
        raise ResponseException('Rule setting must have each of the following: {}'.format(', '.join(rule_dict_keys)),
                                StatusCode.CLIENT_ERROR)

    if rule_dict['impact'] not in RULE_IMPACT_DICT:
        raise ResponseException('Invalid impact: {}'.format(rule_dict['impact']), StatusCode.CLIENT_ERROR)


def save_rule_settings(agency_code, file, errors, warnings):
    """ Given two lists of rules, their settings, agency code, and file, save them in the database.

        Args:
            agency_code: string of the agency's CGAC/FREC code
            file: the rule's file type
            errors: list of error objects and their settings
            warnings: list of warning objects and their settings

        Raises:
            ResponseException if invalid agency code or rule dict
    """
    sess = GlobalDB.db().session

    if (sess.query(CGAC).filter(CGAC.cgac_code == agency_code).count() == 0) and \
            (sess.query(FREC).filter(FREC.frec_code == agency_code).count() == 0):
        raise ResponseException('Invalid agency_code: {}'.format(agency_code), StatusCode.CLIENT_ERROR)

    has_settings = agency_has_settings(sess=sess, agency_code=agency_code, file=file)

    for rule_type, rules in {'fatal': errors, 'warning': warnings}.items():
        # Get the rule ids from the labels
        rule_label_query = file_filter(sess.query(RuleSql.rule_label, RuleSql.rule_sql_id), RuleSql, [file])
        rule_label_query = rule_label_query.filter(RuleSql.rule_severity_id == RULE_SEVERITY_DICT[rule_type])
        rule_label_mapping = {}
        for result in rule_label_query.all():
            rule_label_mapping[result.rule_label] = result.rule_sql_id

        # Compare them with the list provided
        rule_labels = [rule['label'] for rule in rules if 'label' in rule]
        if sorted(rule_labels) != sorted(rule_label_mapping):
            logger.info('{} {}'.format(sorted(rule_labels), sorted(rule_label_mapping)))
            raise ResponseException(
                'Rules list provided doesn\'t match the rules expected: {}'.format(', '.join(rule_labels)),
                StatusCode.CLIENT_ERROR)

        # resetting priorities by the order of the incoming lists
        priority = 1
        for rule_dict in rules:
            validate_rule_dict(rule_dict, rule_label_mapping)
            rule_id = rule_label_mapping[rule_dict['label']]
            impact_id = RULE_IMPACT_DICT[rule_dict['impact']]

            if not has_settings:
                sess.add(RuleSetting(agency_code=agency_code, rule_id=rule_id, priority=priority, impact_id=impact_id))
            else:
                update_params = {'priority': priority, 'impact_id': impact_id}
                sess.query(RuleSetting).filter(RuleSetting.agency_code == agency_code, RuleSetting.rule_id == rule_id).\
                    update(update_params)
            priority += 1
    sess.commit()
    return JsonResponse.create(StatusCode.OK, {'message': 'Agency {} rules saved.'.format(agency_code)})
