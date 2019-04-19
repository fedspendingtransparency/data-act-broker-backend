import datetime
import os

from dataactcore.config import CONFIG_BROKER


def log_job_message(logger, message, job_id=None,
                    is_debug=False, is_warning=False, is_error=False, is_exception=False, other_params={}):
    """Handles logging a message about a validator job, with additional job metadata"""
    log_dict = {
        'message': message,
        'message_type': 'Validator',
        'job_id': job_id,
        'proc_id': os.getpid(),
        'parent_proc_id': os.getppid(),
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

    # TODO: Remove diagnostic code
    # For also logging to a file on an attached volume for later inspection
    mount_drive = os.path.join(CONFIG_BROKER['path'], 'results_drive')
    mount_drive_exists = os.path.exists(mount_drive)
    if mount_drive_exists:
        with open(os.path.join(mount_drive, 'app.log'), 'a') as app_log:
            app_log.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " " + __name__ + ": " +
                          str(log_dict))
