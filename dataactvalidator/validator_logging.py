import os


def log_job_message(logger, message, job_type="Validator", job_id=None,
                    is_debug=False, is_warning=False, is_error=False, is_exception=False, other_params=None):
    """Handles logging a message about a validator job, with additional job metadata"""
    if not other_params:
        other_params = {}

    log_dict = {
        'message': message,
        'job_id': job_id,
        'proc_id': os.getpid(),
        'parent_proc_id': os.getppid(),
    }

    for param in other_params:
        if param not in log_dict:
            log_dict[param] = other_params[param]

    if is_exception:  # use this when handling an exception to include exception details in log output
        log_dict["message_type"] = "{}Error".format(job_type)
        logger.exception(log_dict)
    elif is_error:
        log_dict["message_type"] = "{}Error".format(job_type)
        logger.error(log_dict)
    elif is_warning:
        log_dict["message_type"] = "{}Warning".format(job_type)
        logger.warning(log_dict)
    elif is_debug:
        log_dict["message_type"] = "{}Debug".format(job_type)
        logger.debug(log_dict)
    else:
        log_dict["message_type"] = "{}Info".format(job_type)
        logger.info(log_dict)
