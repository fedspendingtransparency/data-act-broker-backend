from celery import Celery
from dataactcore.config import CONFIG_DB, CONFIG_SERVICES, CONFIG_JOB_QUEUE, CONFIG_BROKER
import requests
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from csv import reader
from dataactcore.utils.jsonResponse import JsonResponse
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


class JobQueue:
    def __init__(self, job_queue_url="localhost"):
        # Set up backend persistent URL
        backendUrl = ''.join(['db+', CONFIG_DB['scheme'], '://', CONFIG_DB['username'], ':', CONFIG_DB['password'], '@',
                              CONFIG_DB['host'], '/', CONFIG_DB['job_queue_db_name']])

        # Set up url to the job queue to establish connection
        queueUrl = ''.join(
            [CONFIG_JOB_QUEUE['broker_scheme'], '://', CONFIG_JOB_QUEUE['username'], ':', CONFIG_JOB_QUEUE['password'],
             '@', job_queue_url, ':', str(CONFIG_JOB_QUEUE['port']), '//'])

        # Create remote connection to the job queue
        self.jobQueue = Celery('tasks', backend=backendUrl, broker=queueUrl)

        @self.jobQueue.task(name='jobQueue.generate_d_file')
        def generate_d_file(api_url, user_id, job_id, interface_holder, timestamped_name, isLocal):
            job_manager = interface_holder().jobDb

            try:
                xml_response = self.get_xml_response_content(api_url)
                url_start_index = xml_response.find("<results>", 0)
                offset = len("<results>")

                if url_start_index == -1:
                    raise ResponseException("Empty response. Validate if input is correct.", StatusCode.CLIENT_ERROR)

                url_start_index += offset
                file_url = xml_response[url_start_index:xml_response.find("</results>", url_start_index)]

                full_file_path = "".join([CONFIG_BROKER['d_file_storage_path'], timestamped_name])

                self.download_file(full_file_path, file_url)
                lines = self.get_lines_from_csv(full_file_path)

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

                job_manager.markJobStatus(job_id, "finished")
                return {"message": "Success", "file_name": file_name}
            except Exception as e:
                # Log the error
                JsonResponse.error(e,500)
                job_manager.getJobById(job_id).error_message = str(e)
                job_manager.markJobStatus(job_id, "failed")
                job_manager.session.commit()
                raise e

        self.enqueue = self.jobQueue.task(name='jobQueue.enqueue')(enqueue)
        self.generate_d_file = generate_d_file

    def get_xml_response_content(self, api_url):
        """ Retrieve XML Response from the provided API url """
        return requests.get(api_url, verify=False).text

    def download_file(self, local_file_path, file_url):
        """ Download a file locally from the specified URL """
        with open(local_file_path, "w") as file:
            # get request
            response = requests.get(file_url)
            # write to file
            response.encoding = "utf-8"
            file.write(response.text)

    def update_d_file_status(self, job_manager, d_file_id, status):
        """ Update the D file status to the one specified via the Job Manager """
        job_manager.setDFileStatus(d_file_id, status)

    def get_lines_from_csv(self, file_path):
        """ Retrieve all lines from specified CSV file """
        lines = []
        with open(file_path) as file:
            for line in reader(file):
                lines.append(line)
        return lines


if __name__ in ['__main__', 'jobQueue']:
    jobQueue = JobQueue()
    queue = jobQueue.jobQueue
