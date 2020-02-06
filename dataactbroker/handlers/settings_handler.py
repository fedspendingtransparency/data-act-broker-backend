import logging

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactcore.interfaces.db import GlobalDB
from dataactbroker.handlers.dashboard_handler import FILE_TYPES
from dataactbroker.helpers.filters_helper import file_filter
from dataactcore.models.lookups import RULE_IMPACT_DICT
from dataactcore.models.domainModels import CGAC, FREC
from dataactcore.models.validationModels import RuleSetting, RuleImpact, RuleSql


logger = logging.getLogger(__name__)


def load_default_rule_settings(sess):
    """ Populates the default rule settings to the database

        Args:
            sess: connection to the database
    """
    priority = 1
    rule_settings = []
    for rule in sess.query(RuleSql.rule_sql_id).order_by(RuleSql.rule_sql_id).all():
        rule_settings.append(RuleSetting(rule_id=rule, agency_code=None, priority=priority,
                                         impact_id=RULE_IMPACT_DICT['high']))
        priority += 1
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
                                     RuleSql.rule_error_message).\
        join(RuleSql, RuleSql.rule_sql_id == RuleSetting.rule_id).\
        join(RuleImpact, RuleImpact.rule_impact_id == RuleSetting.impact_id)
    rule_settings_query = file_filter(rule_settings_query, RuleSql, [file])

    # Filter settings by agency. If they haven't set theirs, use the defaults.
    prev_agency_settings = sess.query(RuleSetting).filter(RuleSetting.agency_code == agency_code).first()
    if prev_agency_settings:
        agency_code_filter = (RuleSetting.agency_code == agency_code)
    else:
        agency_code_filter = RuleSetting.agency_code.is_(None)
    rule_settings_query = rule_settings_query.filter(agency_code_filter)

    # Order by priority/significance
    rule_settings_query = rule_settings_query.order_by(RuleSetting.priority)

    # Note: significance/priority values may still match for the same agency as they are grouped by file types
    # if this grouping is dropped, the significance values between file types will need to be figured out
    rules = []
    significance = 1
    for rule in rule_settings_query.all():
        rules.append({
            'label': rule.rule_label,
            'description': rule.rule_error_message,
            'significance': significance,
            'impact': rule.name
        })
        significance += 1

    return JsonResponse.create(StatusCode.OK, {'rules': rules})
