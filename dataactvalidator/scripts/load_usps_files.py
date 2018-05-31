import argparse
import requests
import json
import logging
import sys
import os
import urllib.parse
import tarfile
import zipfile
import boto3
import shutil
from io import BytesIO

from dataactcore.config import CONFIG_BROKER
from dataactcore.logging import configure_logging

logger = logging.getLogger(__name__)


def get_login_tokens(login_response_body_dict):
    """Creates dictionary of login keys to use to make additional request calls to USPS

       Args:
            login_response_body_dict: response body (type dictionary) returned by the USPS logon endpoint.
       Returns:
            Dictionary containing logon key and token key to use in other requests to the USPS service
    """
    return {"logonkey": login_response_body_dict['logonkey'],
            "tokenkey": login_response_body_dict['tokenkey']}


def get_file_info(download_list_response_body):
    """
       Gets the fileid for the latest version of the zip4 file to be downloaded

       Args:
            download_list_response_body: response body (type dictionary) returned by the USPS download list endpoint.
       Returns:
            fileid as string
    """
    download_list = download_list_response_body['fileList']
    download_list.sort(key=lambda item: item['fulfilled'], reverse=True)
    latest_file = download_list[0]
    return latest_file['fileid']


def get_payload_string(obj_request_body):
    """
        Returns USPS request body format (key: obj, value: {dict_items}

        Args:
            obj_request_body: request body type dictionary
        Returns:
            returns a string formatted to in a way to be understood by the USPS API
    """
    request_body_list = []

    for key, value in obj_request_body.items():
        request_body_list.append('\"{}\":\"{}\"'.format(key, value))

    data_str = "{{{0}}}".format(",\n".join(request_body_list))

    return "obj="+urllib.parse.quote(data_str)


def write_request_to_file(file_path, request):
    """
           Writes request stream to specified file path

           Args:
               file_path: Directory + file name where zip will be written to
               request: requests stream object
           Returns:
               None
    """
    with open(file_path, 'wb') as file:
        for chunk in request.iter_content(chunk_size=1042*1024*10):  # 10MB
            if chunk:
                file.write(chunk)
                file.flush()


def usps_epf_post_request(url, request_body, stream=False, file_path=None):
    """
        Function to make a post request on the USPS rest service

        Args:
            url: endpoint the post request will be made to
            request_body: dictionary containing the
            stream: sets stream parameter to request
            file_path: file path to location where the zip4 tar file from USPS should be written to
        Returns:
            Checks if response was successful by returning the request object to check_response_status()
    """
    headers = {'content-type': 'application/x-www-form-urlencoded', 'cache-control': "no-cache"}
    request = requests.post(url, data=get_payload_string(request_body), headers=headers, stream=stream)
    if stream:
        logger.info('Begin writing stream to file {}'.format(file_path))
        return write_request_to_file(file_path, request)
    return check_response_status(request)


def check_response_status(request):
    """
        Check if the api returns a successful or unsuccessful adjusts program accordingly

        Args:
            request: A request class from the requests package ex: request.post('some url')
        Returns:
            If request is successful then returns response data as a dictionary
        Raises
            exit(1): If request has fails, it logs the error message and exits out of the script
    """
    response_data = json.loads(request.text)

    if request.status_code == requests.codes.ok and response_data['response'] == 'success':
        logger.info('Request successful')
        return response_data
    else:
        logger.error('Request returned error: {}'.format(response_data['messages']))
        sys.exit(1)


def get_login_credentials():
    """
       Logs in to the USPS service and returns tokens necessary to call other USPS endpoints

       Returns:
            A dictionary containing the logon key and token key
    """
    username = CONFIG_BROKER['usps']['username']
    password = CONFIG_BROKER['usps']['password']

    login_url = CONFIG_BROKER['usps']['login']
    usps_credentials = {'login': username, 'pword': password}

    logger.info('Logging in to USPS account')
    login_response_body = usps_epf_post_request(login_url, usps_credentials)
    login_session = get_login_tokens(login_response_body)

    return login_session


def logout(login_session):
    """
       Posts request to logout of USPS service with specific login tokents

       Args:
            login_session: response body (type dictionary) returned by the USPS download list endpoint.
       Returns:
            None
    """
    logger.info('Log out of USPS')
    logout_url = CONFIG_BROKER['usps']['logout']
    usps_epf_post_request(logout_url, login_session)


def extract_zip_from_tar_file(zip_file_path, usps_file_dir):
    """
       Extracts specified zip file from the zip4 tar file from USPS

       Args:
            zip_file_path: File path of zipfile in the tar file that should be extracted
            usps_file_dir: File path of zip4 tar file, the file that will be extracted
       Returns:
            None
    """
    usps_zip_file = os.path.join(usps_file_dir, 'zip4natl.tar')
    zips_tar = tarfile.open(usps_zip_file, mode='r')

    for tar_file_obj in zips_tar:

        if tar_file_obj.name == zip_file_path:
            zips_tar.extract(tar_file_obj, usps_file_dir)

    zips_tar.close()


def extract_upload_zip4_text_files(zip_file_path, extract_file_path, s3_connection=None):
    """
       Extracts text files from the specified zip4 zip file, uploads the files to s3 if bucket provided

       Args:
            zip_file_path: file path of the extracted zip4 zip
            extract_file_path:  file path where the extracted text files will be place
            s3_connection: s3 connection to AWS region
       Returns:
            None
    """
    zip_folder = CONFIG_BROKER["zip_folder"] + "/"
    password = CONFIG_BROKER['usps']['zip4']['password']

    with zipfile.ZipFile(os.path.join(extract_file_path, zip_file_path), 'r') as zip_group:
        for zip_individual in zip_group.namelist():
            with zip_group.open(zip_individual, 'r', password.encode('utf-8')) as zip_nested:
                file_data = BytesIO(zip_nested.read())
                with zipfile.ZipFile(file_data) as zip_text_files:
                    for zip_text in zip_text_files.namelist():
                        logger.info('Extracting file {}'.format(zip_text))
                        zip_text_files.extract(zip_text, extract_file_path, password.encode('utf-8'))
                        if s3_connection:
                            upload_extracted_file_to_s3(s3_connection,
                                                        zip_folder + zip_text,
                                                        extract_file_path + '/' + zip_text)


def extract_upload_city_state_text_file(zip_file_path, extract_file_path, s3_connection=None):
    """
       Extracts text files from the specified city state zip file, uploads the files to s3 if bucket provided

       Args:
            zip_file_path: file path of the extracted zip4 zip
            extract_file_path: file path where the extracted text files will be place
            s3_connection: s3 connection to AWS region
       Returns:
            None
    """

    password = CONFIG_BROKER['usps']['citystate']['password']

    with zipfile.ZipFile(os.path.join(extract_file_path, zip_file_path), 'r') as zip_group:
        for zip_individual in zip_group.namelist():
            zip_group.extract(zip_individual, extract_file_path, password.encode('utf-8'))
            logger.info('Extracting file {}'.format(zip_individual))
            if s3_connection:
                upload_extracted_file_to_s3(s3_connection,
                                            zip_individual,
                                            extract_file_path + '/' + zip_individual)


def upload_extracted_file_to_s3(s3_connection, key_file_path, zip_source_file_path):
    """
       Sends text file to s3 bucket

       Args:
            s3_connection: s3 connection to AWS region
            key_file_path: location (file path + file name) where the file will be uploaded to s3
            zip_source_file_path: local location (file path + file name) where the file to be uploaded is located
       Returns:
            None
    """
    logger.info('Copying {} to s3'.format(zip_source_file_path))
    s3_connection.Object(CONFIG_BROKER['sf_133_bucket'], key_file_path).put(Body=open(zip_source_file_path, 'rb'))


def upload_files_to_s3(args_dict, usps_file_dir):
    """
        Extracts text files (either zip4 or city state) from the downloaded USPS zip4 tar file,
        uploads the files to s3 if bucket provided

        Args:
            args_dict: command line arguments parsed into a dictionary
            usps_file_dir: filepath where USPS zip4 tar file is located
    """
    s3connection = None

    if CONFIG_BROKER["use_aws"]:
        s3connection = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])

    zip4_file_path = 'epf-zip4natl/zip4/zip4.zip'
    city_state_file_path = 'epf-zip4natl/ctystate/ctystate.zip'

    if args_dict.zipcode:
        logger.info('Begin extracting zip4 zip file')
        extract_zip_from_tar_file(zip4_file_path, usps_file_dir)
        logger.info('Begin extracting zip4 text files')
        extract_upload_zip4_text_files(zip4_file_path, usps_file_dir, s3connection)

    if args_dict.city_state:
        logger.info('Begin extracting city state zip file')
        extract_zip_from_tar_file(city_state_file_path, usps_file_dir)
        logger.info('Begin extracting city state text file')
        extract_upload_city_state_text_file(city_state_file_path, usps_file_dir, s3connection)


def download_usps_files(usps_file_dir):
    """
        Downloads the zip4 USPS file from rest service. Requires login to the service, retrieving the id of the most
        recent file, and logs out of the service. Downloads a zip4 tar file in the current directory

        Args:
            usps_file_dir: file path in broker app where zip4 files are extracted
    """
    login_session_obj = get_login_credentials()

    product_codes = {"productcode": CONFIG_BROKER['usps']['product_code'],
                     "productid": CONFIG_BROKER['usps']['product_id']}
    product_codes.update(login_session_obj)

    logger.info('Calling API to retrieve file id')
    files_url = CONFIG_BROKER['usps']['files']
    file_list_response_body = usps_epf_post_request(files_url, product_codes)
    file_id_download = get_file_info(file_list_response_body)

    logout(login_session_obj)

    # Need to login again since token can only be used once
    login_session_obj = get_login_credentials()

    logger.info('Start downloading zipcode file')
    file_code = {"fileid": file_id_download}
    file_code.update(login_session_obj)
    download_url = CONFIG_BROKER['usps']['download']

    if not os.path.exists(usps_file_dir):
        os.makedirs(usps_file_dir)

    usps_zip_file = os.path.join(usps_file_dir, 'zip4natl.tar')
    usps_epf_post_request(download_url, file_code, stream=True, file_path=usps_zip_file)

    logout(login_session_obj)


def main():
    """
        Parses command line arguments either download zip4 data from USPS, upload zip4 data to s3, or upload
        city state data to s3
    """
    parser = argparse.ArgumentParser(description='Download USPS zip4 file and load it to S3')
    parser.add_argument('-d', '--download', help='Download file from USPS', action='store_true')
    parser.add_argument('-z', '--zipcode', help='Upload zipcode files to s3', action='store_true')
    parser.add_argument('-c', '--city_state', help='Upload citystate files to s3', action='store_true')
    parser.add_argument('-r', '--remove', help='Removes files from from USPS directory', action='store_true')
    args = parser.parse_args()

    usps_file_dir = os.path.join(CONFIG_BROKER['path'], 'dataactvalidator', 'config', 'usps')

    if args.download:
        download_usps_files(usps_file_dir)

    upload_files_to_s3(args, usps_file_dir)

    if args.remove:
        shutil.rmtree(usps_file_dir)


if __name__ == '__main__':
    configure_logging()
    main()
