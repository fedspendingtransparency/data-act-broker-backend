from celery import Celery
from dataactcore.config import CONFIG_DB, CONFIG_SERVICES, CONFIG_JOB_QUEUE, CONFIG_BROKER
import requests
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from csv import reader
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
import os


class JobQueue:
    def __init__(self, job_queue_url="localhost"):
        # Set up backend persistent URL
        backendUrl = ''.join(['db+', CONFIG_DB['scheme'], '://', CONFIG_DB['username'], ':', CONFIG_DB['password'], '@',
                              CONFIG_DB['host'], '/', CONFIG_DB['job_queue_db_name']])

        # Set up url to the validator for the RESTFul calls
        validatorUrl = ''.join([CONFIG_SERVICES['validator_host'], ':', str(CONFIG_SERVICES['validator_port'])])
        if 'http://' not in validatorUrl:
            validatorUrl = ''.join(['http://', validatorUrl])

        # Set up url to the job queue to establish connection
        queueUrl = ''.join(
            [CONFIG_JOB_QUEUE['broker_scheme'], '://', CONFIG_JOB_QUEUE['username'], ':', CONFIG_JOB_QUEUE['password'],
             '@', job_queue_url, ':', str(CONFIG_JOB_QUEUE['port']), '//'])

        # Create remote connection to the job queue
        self.jobQueue = Celery('tasks', backend=backendUrl, broker=queueUrl)

        @self.jobQueue.task(name='jobQueue.enqueue')
        def enqueue(jobID):
            # Don't need to worry about the response currently
            url = ''.join([validatorUrl, '/validate/'])
            params = {
                'job_id': jobID
            }
            response = requests.post(url, params)
            return response.json()

        @self.jobQueue.task(name='jobQueue.generate_d_file')
        def generate_d_file(api_url, user_id, d_file_id, interface_holder, timestamped_name, isLocal):
            job_manager = interface_holder().jobDb

            try:
                xml_response = str(requests.get(api_url, verify=False).content)
                url_start_index = xml_response.find("<results>", 0)
                offset = 9

                if url_start_index == -1:
                    raise ResponseException("Empty response. Validate if input is correct.", StatusCode.CLIENT_ERROR)

                url_start_index += offset
                file_url = xml_response[url_start_index:xml_response.find("</results>", url_start_index)]

                full_file_path = "".join([CONFIG_BROKER['d_file_storage_path'], timestamped_name])

                with open(full_file_path, "w") as file:
                    # get request
                    response = requests.get(file_url)
                    # write to file
                    response.encoding = "utf-8"
                    file.write(response.text)

                lines = []
                with open(full_file_path) as file:
                    for line in reader(file):
                        lines.append(line)

                headers = lines[0]

                if isLocal:
                    file_name = "".join([CONFIG_BROKER['broker_files'], timestamped_name])
                    csv_writer = CsvLocalWriter(file_name, headers)
                else:
                    file_name = "".join([str(user_id), "/", timestamped_name])
                    bucket = CONFIG_BROKER['aws_bucket']
                    region = CONFIG_BROKER['aws_region']
                    csv_writer = CsvS3Writer(region, bucket, file_name, headers)

                with csv_writer as writer:
                    for line in lines[1:]:
                        writer.write(line)
                    writer.finishBatch()

                job_manager.setDFileStatus(d_file_id, "finished")
                return {"message": "Success", "file_name": file_name}
            except Exception as e:
                # Log the error
                JsonResponse.error(e,500)
                job_manager.setDFileMessage(d_file_id, str(e))
                job_manager.setDFileStatus(d_file_id, "failed")
                raise e

        self.enqueue = enqueue
        self.generate_d_file = generate_d_file


if __name__ in ['__main__', 'jobQueue']:
    jobQueue = JobQueue()
    queue = jobQueue.jobQueue