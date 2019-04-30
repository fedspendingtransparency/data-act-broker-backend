import sys
import os
import requests
import logging
import re
import boto3
import urllib.request
from bs4 import BeautifulSoup
import zipfile
import datetime

from dataactcore.config import CONFIG_BROKER
from dataactcore.logging import configure_logging

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)
BASE_URL = 'https://geonames.usgs.gov'


def get_download_url():
    """ Does the actual scraping and finds the url to the download (including the file name)

        Returns:
            Download URL and name (with date) of the file
    """
    logger.info('Finding download URL.')
    site_url = 'https://www.usgs.gov/core-science-systems/ngp/board-on-geographic-names/download-gnis-data'
    response = requests.get(site_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    links = soup.find_all('a')

    # Look through the links until we find the one we need to download.
    for link in links:
        if link.get('href') and 'NationalFedCodes.zip' in link.get('href'):
            return link.get('href')

    return None, None


def download_and_extract_file(file_dir, file_url):
    """ Download the file from the given URL, extract the txt from the zip, delete the zip.

        Args:
            file_dir: string indicating the directory the file is stored in
            file_url: the url to download the file from
    """
    logger.info('Downloading zip file.')
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)

    city_zip_file = os.path.join(file_dir, 'NationalFedCodes.zip')
    urllib.request.urlretrieve(file_url, city_zip_file)

    logger.info('Extracting zip contents.')
    with zipfile.ZipFile(city_zip_file, 'r') as zip_file:
        zip_file.extractall(file_dir)

    os.remove(city_zip_file)


def main():
    """ Scrapes geonames.usgs.gov to find the city file and download it. """
    start_time = datetime.datetime.now()
    logger.info('Starting NationalFedCodes file retrieval.')
    city_file_url = get_download_url()

    # If a URL wasn't found, we want to exit without doing anything else
    if not city_file_url:
        logger.error('No valid NationalFedCodes download url located')
        sys.exit(1)

    city_file_dir = os.path.join(CONFIG_BROKER['path'], 'dataactvalidator', 'config')
    download_and_extract_file(city_file_dir, city_file_url)

    # Find the file we just downloaded, can't have any other versions of the file in the folder for this to work
    file_name = ''
    for dir_file in os.listdir(city_file_dir):
        if re.match('NationalFedCodes_\d{8}\.txt', dir_file):
            file_name = dir_file

    city_file_path = os.path.join(city_file_dir, file_name)

    # Simply rename the file if it's local, upload and delete from local if not
    if not CONFIG_BROKER['use_aws']:
        os.rename(city_file_path, os.path.join(city_file_dir, 'NationalFedCodes.txt'))
    else:
        logger.info('Uploading NationalFedCodes.txt to S3.')
        s3_resource = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])
        s3_resource.Object(CONFIG_BROKER['sf_133_bucket'], 'NationalFedCodes.txt').put(Body=open(city_file_path, 'rb'))
        # Still need to remove the file so when we get a new one we know which one to use
        os.remove(city_file_path)

    logger.info('NationalFedCodes file retrieval completed in {} seconds.'.format(datetime.datetime.now() - start_time))


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
