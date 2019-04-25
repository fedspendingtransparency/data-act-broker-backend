import sys
import os
import requests
import logging
import re
import boto3
import urllib.request
from bs4 import BeautifulSoup
import zipfile

from dataactcore.config import CONFIG_BROKER
from dataactcore.logging import configure_logging

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)
BASE_URL = 'https://geonames.usgs.gov'


def main():
    """ Scrapes geonames.usgs.gov to find the city file and download it. """

    city_file_dir = os.path.join(CONFIG_BROKER['path'], 'dataactvalidator', 'config')

    response = requests.get(BASE_URL + '/domestic/download_data.htm')
    soup = BeautifulSoup(response.content, 'html.parser')

    links = soup.find_all('a')
    city_file_link = ''
    file_name = ''

    for link in links:
        matching = re.match('^/docs/federalcodes/(NationalFedCodes_\d{8})\.zip$', link.get('href'))
        if matching:
            city_file_link = BASE_URL + link.get('href')
            file_name = matching.group(1)
            break

    if not city_file_link:
        logger.error("No valid NationalFedCodes download url located")
        sys.exit(1)

    if not os.path.exists(city_file_dir):
        os.makedirs(city_file_dir)

    city_zip_file = os.path.join(city_file_dir, 'NationalFedCodes.zip')
    urllib.request.urlretrieve(city_file_link, city_zip_file)

    with zipfile.ZipFile(city_zip_file, "r") as zip_ref:
        zip_ref.extractall(city_file_dir)

    os.remove(city_zip_file)

    city_file_path = os.path.join(city_file_dir, file_name + ".txt")

    if not CONFIG_BROKER['use_aws']:
        os.rename(city_file_path, os.path.join(city_file_dir, "NationalFedCodes.txt"))
    else:
        s3_resource = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])
        s3_resource.Object(CONFIG_BROKER['sf_133_bucket'], "NationalFedCodes.txt").put(Body=open(city_file_path, 'rb'))
        os.remove(city_file_path)


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
