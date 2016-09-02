from contextlib import contextmanager
from csv import reader

from celery import Celery

from dataactcore.config import (
    CONFIG_DB, CONFIG_SERVICES, CONFIG_JOB_QUEUE, CONFIG_BROKER)
import requests
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils import fileF
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode


def enqueue(jobID):
    """POST a job to the validator"""
    validatorUrl = '{validator_host}:{validator_port}'.format(
        **CONFIG_SERVICES)
    if 'http://' not in validatorUrl:
        validatorUrl = 'http://' + validatorUrl
    validatorUrl += '/validate/'
    params = {
        'job_id': jobID
    }
    response = requests.post(validatorUrl, params)
    return response.json()


def get_xml_response_content(api_url):
    """ Retrieve XML Response from the provided API url """
    return requests.get(api_url, verify=False).text


def download_file(local_file_path, file_url):
    """ Download a file locally from the specified URL """
    with open(local_file_path, "w") as file:
        # get request
        response = requests.get(file_url)
        # write to file
        response.encoding = "utf-8"
        file.write(response.text)


def get_lines_from_csv(file_path):
    """ Retrieve all lines from specified CSV file """
    lines = []
    with open(file_path) as file:
        for line in reader(file):
            lines.append(line)
    return lines


def write_csv(user_id, file_name, is_local, header, body):
    """Derive the relevant location and write a CSV to it.
    :return: the final file name (complete with prefix)"""
    if is_local:
        file_name = CONFIG_BROKER['broker_files'] + file_name
        csv_writer = CsvLocalWriter(file_name, header)
    else:
        file_name = "{}/{}".format(user_id, file_name)
        bucket = CONFIG_BROKER['aws_bucket']
        region = CONFIG_BROKER['aws_region']
        csv_writer = CsvS3Writer(region, bucket, file_name, header)

    with csv_writer as writer:
        for line in body:
            writer.write(line)
        writer.finishBatch()
    return file_name


@contextmanager
def exception_logging_jobDb(interface_holder_class, job_id):
    """If something goes wrong with this job, log the exception"""
    interfaces = interface_holder_class()
    job_manager = interfaces.jobDb
    try:
        yield job_manager
    except Exception as e:
        # Log the error
        JsonResponse.error(e, 500)
        job_manager.getJobById(job_id).error_message = str(e)
        job_manager.markJobStatus(job_id, "failed")
        job_manager.session.commit()
        raise e
    finally:
        interfaces.close()


def generate_d_file(api_url, user_id, job_id, interface_holder,
                    timestamped_name, is_local):
    with exception_logging_jobDb(interface_holder, job_id) as job_manager:
        xml_response = get_xml_response_content(api_url)
        url_start_index = xml_response.find("<results>", 0)
        offset = len("<results>")

        if url_start_index == -1:
            raise ResponseException(
                "Empty response. Validate if input is correct.",
                StatusCode.CLIENT_ERROR)

        url_start_index += offset
        url_end_index = xml_response.find("</results>", url_start_index)
        file_url = xml_response[url_start_index:url_end_index]

        full_file_path = (CONFIG_BROKER['d_file_storage_path'] +
                          timestamped_name)

        download_file(full_file_path, file_url)
        lines = get_lines_from_csv(full_file_path)

        file_name = write_csv(user_id, timestamped_name, is_local,
                              header=lines[0], body=lines[1:])

        job_manager.markJobStatus(job_id, "finished")
        return {"message": "Success", "file_name": file_name}


def generate_f_file(submission_id, user_id, job_id, interface_holder,
                    timestamped_name, is_local):
    with exception_logging_jobDb(interface_holder, job_id) as job_manager:
        rows_of_dicts = fileF.generateFRows(job_manager.session, submission_id)
        header = [key for key in fileF.mappings]    # keep order
        body = []
        for row in rows_of_dicts:
            body.append([row[key] for key in header])

        file_name = write_csv(user_id, timestamped_name, is_local,
                              header=header, body=body)
        job_manager.markJobStatus(job_id, "finished")
        return {"message": "Success", "file_name": file_name}


class JobQueue:
    def __init__(self, job_queue_url="localhost"):
        # Set up backend persistent URL
        backendUrl = ('db+{scheme}://{username}:{password}@{host}'
                      '/{job_queue_db_name}').format(**CONFIG_DB)

        # Set up url to the job queue to establish connection
        queueUrl = ('{broker_scheme}://{username}:{password}@{host}:{port}'
                    '//').format(host=job_queue_url, **CONFIG_JOB_QUEUE)

        # Create remote connection to the job queue
        self.jobQueue = Celery('tasks', backend=backendUrl, broker=queueUrl)

        self.enqueue = self.jobQueue.task(name='jobQueue.enqueue')(enqueue)
        self.generate_d_file = self.jobQueue.task(
            name='jobQueue.generate_d_file')(generate_d_file)
        self.generate_f_file = self.jobQueue.task(
            name='jobQueue.generate_f_file')(generate_f_file)


if __name__ in ['__main__', 'jobQueue']:
    jobQueue = JobQueue()
    queue = jobQueue.jobQueue
