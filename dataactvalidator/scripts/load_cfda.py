from ftplib import FTP
from io import BytesIO
from dataactcore.config import CONFIG_BROKER
from dataactcore.logging import configure_logging

import boto3


def load_cfda():
    # connect to host, default port
    ftp = FTP('ftp.cfda.gov')
    # anonymous FTP, the archive site allow general access
    # user anonymous, password anonymous
    ftp.login()

    # change the directory to /usaspending/
    ftp.cwd('usaspending')

    data = []

    # output the directory contents
    ftp.dir('-t', data.append)

    # get the most recent updated cfda file
    file_listing = data[0]

    # break string down by adding the data to a string array using a space separator
    # example of the list: "22553023 May 07 01:51 programs-full-usaspending17126.csv"
    file_parts = file_listing.split(' ')

    # get the last string array (file name)
    file_name = file_parts[-1]

    print('Loading ' + file_name)

    if CONFIG_BROKER["use_aws"]:
        data = BytesIO()
        # download file
        ftp.retrbinary('RETR ' + file_name, data.write)
        data.seek(0)

        s3 = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])
        s3.Bucket(CONFIG_BROKER['sf_133_bucket']).put_object(Key='cfda_program.csv', Body=data)

        print('Loading file to S3 completed')
    else:
        # download file
        ftp.retrbinary('RETR ' + file_name, open('dataactvalidator/config/cfda_program.csv', 'wb').write)
        print('Loading file completed')

    ftp.quit()


if __name__ == '__main__':
    configure_logging()
    load_cfda()
