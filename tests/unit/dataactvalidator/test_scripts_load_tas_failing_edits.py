import os
from decimal import Decimal

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import TASFailedEdits
from dataactvalidator.scripts import load_tas_failing_edits


def test_load_tas_failing_edits_file(database):
    """ This function loads the data from a local file to the database """
    sess = database.session
    failed_tas_path = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'GTAS_FE_DA_202112.csv')

    load_tas_failing_edits.load_tas_failing_edits_file(sess, failed_tas_path, 2021, 5)

    # We should have loaded eight rows
    assert sess.query(TASFailedEdits).count() == 6

    # Picking one of the lines to do spot checks on
    single_check = sess.query(TASFailedEdits).filter_by(agency_identifier='372').one()
    assert single_check.display_tas == '372-X-8296-000'
    assert single_check.severity == 'Fatal'
    assert single_check.atb_submission_status == 'C'
    assert single_check.approved_override_exists is True
    assert single_check.fr_entity_type == '9521'
    assert single_check.fr_entity_description == 'Harry S. Truman Scholarship Trust Fund'
    assert single_check.edit_number == '20'
