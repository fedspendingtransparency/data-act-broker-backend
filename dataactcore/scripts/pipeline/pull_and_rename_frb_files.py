import boto3
import datetime
import os
import re
import argparse
import pandas as pd
from collections import namedtuple

'''
Pulls and renames both the most recent FRB files.
By default, does nothing if there was no S3 files found in the past 24 hours, 
can be forced to pull the most recent.
'''
DataSource = namedtuple('DataSource', ['name', 'full_name', 'old_filename', 'new_filename'])
DATA_TYPES = [
    DataSource(
        name='cars',
        full_name='CARS',
        old_filename=r'^PE\.CARS\_DA\-(\d{4})(\d{2}).*$',  # ex: PE.CARS_DA-202508-01.txt
        new_filename='cars_tas.csv'),
    DataSource(
        name='sf133',
        full_name='GTAS',
        old_filename=r'^PE\.GTAS\_DA\_(\d{4})(\d{2}).*$',  # ex: PE.GTAS_DA_202508_1749207621899.txt
        new_filename='sf_133_{year}_{period}.csv'),
    DataSource(
        name='failed-edits',
        full_name="GTAS Failed Edits",
        old_filename=r'^GTAS\_FE\_DA\_(\d{4})(\d{2}).*$',  # ex: GTAS_FE_DA_202507_1747742614873.txt
        new_filename='GTAS_FE_DA_{year}{period}.csv'),
    DataSource(
        name='boc',
        full_name="GTAS BOC",
        old_filename=r'^OMB\_Extract\_BOC\_(\d{4})(\d{2}).*$',  # ex: OMB_Extract_BOC_202507_1747742622897.txt
        new_filename='OMB_Extract_BOC_{year}_{period}.csv')
]
DATA_TYPES_NAMES = [data_type.name for data_type in DATA_TYPES]
DATA_TYPES_DICT = {source.name: source for source in DATA_TYPES}

# Helper function...if the contents of the row aren't empty and "null" is in it, delete the contents of the row
def replace_null(row, column_name):
    if row[column_name] and 'null' in row[column_name]:
        return None
    return row[column_name]


def check_for_new_file(recent_files, bucket_source, data_source):
    pulled = False

    found_files = [re.search(data_source.old_filename, x) for x in recent_files
                   if re.search(data_source.old_filename, x)]
    if found_files:
        pulled = True
        found_file = found_files[0]
        old_filename = found_file.string
        year, period = found_file.group(1), found_file.group(2)
        if data_source.name != 'cars':
            new_filename = data_source.new_filename.format(year=year, period=period)
        else:
            new_filename = data_source.new_filename

        print(f'Downloading {old_filename} as {new_filename}')
        os.makedirs('files', exist_ok=True)
        bucket_source.download_file(old_filename, os.path.join(os.getcwd(), 'files', new_filename))
        print(f'{data_source.full_name} download successful')
    else:
        print(f'No {data_source.full_name} file posted in the last 24 hours, or no files found.')

    return pulled

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--force_pull_latest', action='store_true', help='Pulls the most recent file,'
                                                                         ' regardless of when it came in.')
    parser.add_argument('--data_types', nargs='+', choices=DATA_TYPES_NAMES + ['all'],
                        help='The data types to pull. Use \'all\' to include everything (default).',
                        default=['all'])
    parser.add_argument('--bucket', nargs='?', const='default', default='default', type=str)
    args = parser.parse_args()

    print(f'\nPulling latest from bucket "{args.bucket}".')

    last_run = (datetime.datetime.now() - datetime.timedelta(hours=24)).strftime("%Y-%m-%d %H:%m:%S")
    if not args.force_pull_latest:
        print(f'Ignoring files modified before {last_run}')

    s3 = boto3.resource(service_name='s3', region_name='us-gov-west-1')
    bucket_source = s3.Bucket(args.bucket)

    all_files = bucket_source.objects.all()

    if args.force_pull_latest:
        recent_files = list(all_files)
    else:
        recent_files = [x for x in all_files if (
            datetime.datetime.utcnow() - x.last_modified.replace(tzinfo=None) <
            datetime.timedelta(hours=24))]

    recent_files.sort(key=lambda tup: tup.last_modified, reverse=True)
    recent_files = [x.key for x in recent_files]

    pulled_file = False
    data_types = args.data_types if 'all' not in args.data_types else DATA_TYPES_NAMES
    for data_type in data_types:
        data_source = DATA_TYPES_DICT[data_type]
        new_file = check_for_new_file(recent_files, bucket_source, data_source)
        pulled_file |= new_file

        if data_source.name == 'cars' and new_file:
            # read CGAC values from csv
            cars_csv = os.path.join('files', data_source.new_filename)
            data = pd.read_csv(cars_csv, dtype=str, keep_default_na=False)

            data['End Date'] = data.apply(lambda x: replace_null(x, 'End Date'), axis=1)
            data['Financial Indicator Type 2'] = data.apply(
                lambda x: replace_null(x, 'Financial Indicator Type 2'),
                axis=1
            )

            data.rename(columns={'FR Entity Type Code': 'FR Entity Type',
                                 'Financial Indicator Type 2': 'financial_indicator_type2',
                                 'Date/Time Established': 'DT_TM_ESTAB',
                                 'End Date': 'DT_END'},
                                 inplace=True)
            data.to_csv(cars_csv)

    if not pulled_file:
        print(f'No files in {args.bucket} modified since {last_run}, or no files found.')

if __name__ == '__main__':
    main()
