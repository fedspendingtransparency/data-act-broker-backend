import os
import logging
import re
import boto3
import urllib.request
import zipfile
import datetime
import argparse
import shutil

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

FED_CODES_URL = 'https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/FederalCodes/FedCodes_National_Text.zip'
GOV_UNITS_URL = 'https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/Topical/GovernmentUnits_National_Text.zip'
FED_CODES_FILENAME = 'FederalCodes_National'
GOV_UNITS_FILENAME = 'GovernmentUnits_National'


def download_and_extract_file(file_dir, url, zip_name, file_name):
    """ Download the file from the url and extract the txt from the zip

        Args:
            file_dir: string indicating the directory the file is stored in
            url: url to pull from
            zip_name: name of zip to unzip
            file_name: name of the file to be pulled

        Returns:
            path to extracted file
    """
    file_path = None
    subfolder_path = os.path.join(file_dir, 'Text')

    logging.info('Downloading {}'.format(zip_name))
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    zip_path = os.path.join(file_dir, zip_name)
    urllib.request.urlretrieve(url, zip_path)

    logging.info('Extracting zip contents')
    with zipfile.ZipFile(zip_path, 'r') as zip_file:
        zip_file.extractall(file_dir)
    os.remove(zip_path)

    # Find the file we just downloaded, can't have any other versions of the file in the folder for this to work
    logging.info('Finding {}'.format(file_name))
    for dir_file in os.listdir(subfolder_path):
        if re.match(file_name, dir_file):
            file_path = os.path.join(subfolder_path, file_name)

    return file_path


def get_parser():
    parser = argparse.ArgumentParser(description="Get NationalFedCodes file from USGS")
    parser.add_argument("--bucket", type=str, default=None, help='S3 bucket to upload it to')
    parser.add_argument("--fed-codes", action='store_true', default=False, help='Extract the fed codes file')
    parser.add_argument("--gov-units", action='store_true', default=False, help='Extract the gov units file')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    bucket = args.bucket
    fed_codes = args.fed_codes
    gov_units = args.gov_units

    files = {}
    if fed_codes:
        files[FED_CODES_URL] = FED_CODES_FILENAME
    if gov_units:
        files[GOV_UNITS_URL] = GOV_UNITS_FILENAME
    if not files:
        logging.error('Please specify --fed-codes and/or --gov-units')
        exit()

    start_time = datetime.datetime.now()
    logging.info('Starting USGS file retrieval.')

    temp_extract_dir = 'temp_extract_dir'
    for url, filename in files.items():
        base_name = url[url.rfind("/") + 1:url.rfind(".")]
        file_name = '{}.txt'.format(filename)
        file_path = download_and_extract_file(temp_extract_dir, url, '{}.zip'.format(base_name), file_name)

        if file_path:
            if bucket:
                logging.info('Uploading {} to S3.'.format(file_name))
                s3_resource = boto3.resource('s3', region_name='us-gov-west-1')
                s3_resource.Object(bucket, 'broker_reference_data/{}'.format(file_name)).put(Body=open(file_path, 'rb'))
                os.remove(file_path)
            else:
                os.rename(file_path, file_name)
        else:
            logging.info('{} not found.'.format(file_name))

    shutil.rmtree(temp_extract_dir)

    logging.info('USGS file retrieval completed in {} seconds.'.format(datetime.datetime.now() - start_time))
