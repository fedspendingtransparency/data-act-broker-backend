import re
import os

from dataactcore.config import CONFIG_BROKER
from dataactvalidator.scripts import load_sf133

SF_RE = re.compile(r'sf_133_(?P<year>\d{4})_(?P<period>\d{2})\.csv')


def test_sf133_files(database):
    """Test sums of all TAS's in any unloaded SF-133 files"""
    failed_validations = ['file,aid,ata,availability_type_code,bpoa,epoa,main_account,sub_account,'
                          'error_type,value1,value2']

    # get a list of SF 133 files to test
    sf133_list = load_sf133.get_sf133_list(os.path.join(CONFIG_BROKER['path'], 'dataactvalidator', 'config',
                                                        'to_validate'))

    # test data in each SF 133 file
    for sf133 in sf133_list:

        # skipping files with invalid name
        file_match = SF_RE.match(sf133.file)
        if file_match:
            file_index = "{0}_{1}".format(file_match.group('year'), file_match.group('period'))

            data = load_sf133.clean_sf133_data(sf133.full_file, None)

            # remove all lines not included in validation
            validate_one_lines = [
                '1000', '1010', '1011', '1012', '1013', '1014', '1015', '1016', '1017', 
                '1018', '1019', '1020', '1021', '1022', '1023', '1024', '1025', '1026', 
                '1027', '1028', '1029', '1030', '1031', '1032', '1033', '1034', '1035', 
                '1036', '1037', '1038', '1039', '1040', '1041', '1042', '1160', '1180', 
                '1260', '1280', '1340', '1440', '1540', '1640', '1750', '1850'
            ]
            validate_two_lines = ['2190', '2490']
            validation_sum_lines = ['1910', '2500']

            data = data[(data.line.isin(validate_one_lines + validate_two_lines + validation_sum_lines) &
                         (data.amount != 0))]

            # sort data by unique TAS
            data.sort_values(by=['tas'], inplace=True)
            data.set_index(keys=['tas'], drop=False, inplace=True)

            # iterate through data by TAS
            for key, tas in data.groupby(['tas']):
                sum_one, validate_one = 0, 0
                sum_two, validate_two = 0, 0

                for index, row in tas.iterrows():
                    if row.line in validate_one_lines:
                        sum_one += row.amount
                    elif row.line == '1910':
                        validate_one = float("{0:.2f}".format(row.amount))
                    elif row.line in validate_two_lines:
                        sum_two += row.amount
                    elif row.line == '2500':
                        validate_two = float("{0:.2f}".format(row.amount))
                sum_one = float("{0:.2f}".format(sum_one))
                sum_two = float("{0:.2f}".format(sum_two))

                validation_one = sum_one == validate_one
                validation_two = sum_two == validate_two
                validation_three = validate_one == validate_two

                current_tas = tas.iloc[0]
                join_array = [
                    file_index, current_tas['agency_identifier'], 
                    current_tas['allocation_transfer_agency'], current_tas['availability_type_code'], 
                    current_tas['beginning_period_of_availa'], current_tas['ending_period_of_availabil'], 
                    current_tas['main_account_code'], current_tas['sub_account_code']
                ]

                if not (validation_one and validation_two) and \
                        (not current_tas.empty and not current_tas['availability_type_code'] == 'X' and
                            int(current_tas['fiscal_year']) > int(current_tas['ending_period_of_availabil'])):
                    # line 1910 != line 2500
                    if not validation_three:
                        failed_validations.append(','.join(join_array + [
                            '1900!=2500', "{0:.2f}".format(validate_one), "{0:.2f}".format(validate_two)
                        ]))
                else:
                    # line 1910 != sum of validation_one_lines
                    if not validation_one:
                        failed_validations.append(','.join(join_array + [
                            'sum!=1910', "{0:.2f}".format(sum_one), "{0:.2f}".format(validate_one)
                        ]))
                    # line 2500 != sum of validation_two_lines
                    if not validation_two:
                        failed_validations.append(','.join(join_array + [
                            'sum!=2500', "{0:.2f}".format(sum_two), "{0:.2f}".format(validate_two)
                        ]))

    assert len(failed_validations) == 1, "\n".join(str(failure) for failure in failed_validations)
