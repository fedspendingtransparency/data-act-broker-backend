import os
import pandas as pd

from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app


# if the contents of the row aren't empty and "null" is in it, delete the contents of the row
def replace_null(row, column_name):
    if row[column_name] and 'null' in row[column_name]:
        return None
    return row[column_name]


def main():
    current_dir = os.getcwd()
    cars_file_name = os.path.join(current_dir, "cars_tas.csv")

    # read CGAC values from csv
    data = pd.read_csv(cars_file_name, dtype=str)

    data['End Date'] = data.apply(lambda x: replace_null(x, 'End Date'), axis=1)
    data.rename(columns={'FR Entity Type Code': 'FR Entity Type',
                         'Date/Time Established': 'DT_TM_ESTAB',
                         'End Date': 'DT_END'},
                inplace=True)
    data.to_csv('cars_tas.csv')

if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()