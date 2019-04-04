from os import getpid, getppid

import psutil
import uuid
from flask import current_app, g, _app_ctx_stack

from dataactcore.interfaces.db import GlobalDB


# ============================================================
# DIAGNOSTIC CODE
# - to to be used while under test, then removed
# ============================================================
def ensure_db_session_key(session):
    # info is a dictionary local to the session object
    if 'db_session_key' not in session.info:
        session.info['db_session_key'] = uuid.uuid4().hex


def log_session_size(logger, job_id=None, checkpoint_name='<unspecified>'):
    sess = GlobalDB.db().session
    ensure_db_session_key(sess)
    message = "Diagnostic on SQLAlchemy Session object [{}] at [{}]".format(
        sess,
        checkpoint_name,
    )
    log_metadata_dict = {'memory': dict(psutil.Process().memory_full_info()._asdict())}
    log_job_message(logger, message, job_id, is_debug=True, other_params=log_metadata_dict)
# ============================================================


def log_job_message(logger, message, job_id=None,
                    is_debug=False, is_warning=False, is_error=False, is_exception=False,
                    other_params={}):
    """Handles logging a message about a validator job, with additional job metadata"""
    log_dict = {
        'proc_id': getpid(),
        'parent_proc_id': getppid(),
        'job_id': job_id,
        'current_app': hex(id(current_app)) if current_app else None,
        'flask.g': hex(id(g)) if g else None,
        '_app_ctx_stack.__ident_func__': hex(_app_ctx_stack.__ident_func__()) if _app_ctx_stack else None,
        'db_session_key': GlobalDB.db().session.info['db_session_key'] if 'db_session_key' in
                                                                          GlobalDB.db().session.info else None,
        'db_session': hex(id(GlobalDB.db().session)),
        'message': message,
        'message_type': 'Validator'
    }

    for param in other_params:
        if param not in log_dict:
            log_dict[param] = other_params[param]

    if is_exception:  # use this when handling an exception to include exception details in log output
        log_dict["message_type"] = "ValidatorError"
        logger.exception(log_dict)
    elif is_error:
        log_dict["message_type"] = "ValidatorError"
        logger.error(log_dict)
    elif is_warning:
        log_dict["message_type"] = "ValidatorWarning"
        logger.warning(log_dict)
    elif is_debug:
        log_dict["message_type"] = "ValidatorDebug"
        logger.debug(log_dict)
    else:
        log_dict["message_type"] = "ValidatorInfo"
        logger.info(log_dict)