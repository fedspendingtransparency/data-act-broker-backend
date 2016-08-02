from celery import Celery
from dataactcore.config import CONFIG_DB, CONFIG_SERVICES, CONFIG_JOB_QUEUE, CONFIG_BROKER
import requests
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from csv import reader
from dataactcore.aws.s3UrlHandler import s3UrlHandler

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

        @self.jobQueue.task(name='jobQueue.generate_d1')
        def generate_d_file(api_url, file_name, user_id, d_file_id, interface_holder):
            job_manager = interface_holder().jobDb
            try:
                job_manager.setDFileStatus(d_file_id, "waiting")

                xml_response = str(requests.get(api_url, verify=False).content)
                url_start_index = xml_response.find("<results>", 0) + 9
                file_url = xml_response[url_start_index:xml_response.find("</results>", url_start_index)]

                bucket = CONFIG_BROKER['aws_bucket']
                region = CONFIG_BROKER['aws_region']

                aws_file_name = "".join([str(user_id), "/", file_name])

                with open(file_name, "wb") as file:
                    # get request
                    response = requests.get(file_url)
                    # write to file
                    file.write(response.content)

                lines = []
                with open(file_name) as file:
                    for line in reader(file):
                            lines.append(line)

                headers = lines[0]
                with CsvS3Writer(region, bucket, aws_file_name, headers) as writer:
                    for line in lines[1:]:
                        writer.write(line)
                        writer.finishBatch()

                s3_url = s3UrlHandler().getSignedUrl(path=user_id, fileName=file_name)
                job_manager.setDFileUrl(d_file_id, s3_url)
                job_manager.setDFileStatus(d_file_id, "finished")
            except Exception as e:
                job_manager.setDFileMessage(d_file_id, e.message)
                job_manager.setDFileStatus(d_file_id, "failed")

        self.enqueue = enqueue
        self.generate_d_file = generate_d_file


if __name__ in ['__main__', 'jobQueue']:
    jobQueue = JobQueue()
    queue = jobQueue.jobQueue
