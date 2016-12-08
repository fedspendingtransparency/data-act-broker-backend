import logging
import re
import pandas as pd

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import SF133
from dataactvalidator.scripts import load_sf133
from dataactvalidator.scripts.loaderUtils import LoaderUtils
from tests.unit.dataactcore.factories.domain import SF133Factory

SF_RE = re.compile(r'sf_133_(?P<year>\d{4})_(?P<period>\d{2})\.csv')
TAS_RE = re.compile(r'(?P<ata>\d{3})(?P<aid>\d{3})(?P<bpoa>\d{4})(?P<epoa>\d{4})(?P<atc>\D{1})(?P<mac>\d{4})(?P<sac>\d{3})')



def test_sf133_files(database):
    """Test sums of all TAS's in any unloaded SF-133 files"""
    failed_validations = ['file,aid,ata,availability_type_code,bpoa,epoa,main_account,sub_account,error_type,value1,value2']

    # get a list of SF 133 files to test
    sf133_list = load_sf133.get_sf133_list(None)

    # test data in each SF 133 file
    for sf133 in sf133_list:

        # skipping files with invalid name
        file_match = SF_RE.match(sf133.file)
        if file_match:
            file_index = "{0}_{1}".format(file_match.group('year'), file_match.group('period'))
            file_fiscal_year = int(file_match.group('year'))
            if int(file_match.group('period'))>10:
                file_fiscal_year += 1

            data = pd.read_csv(sf133.full_file, dtype=str)
            data = LoaderUtils.cleanData(
                data,
                SF133,
                {"ata": "allocation_transfer_agency",
                 "aid": "agency_identifier",
                 "availability_type_code": "availability_type_code",
                 "bpoa": "beginning_period_of_availa",
                 "epoa": "ending_period_of_availabil",
                 "main_account": "main_account_code",
                 "sub_account": "sub_account_code",
                 "fiscal_year": "fiscal_year",
                 "period": "period",
                 "line_num": "line",
                 "amount_summed": "amount"},
                {"allocation_transfer_agency": {"pad_to_length": 3},
                 "agency_identifier": {"pad_to_length": 3},
                 "main_account_code": {"pad_to_length": 4},
                 "sub_account_code": {"pad_to_length": 3},
                 # next 3 lines handle the TAS fields that shouldn't
                 # be padded but should still be empty spaces rather
                 # than NULLs. this ensures that the downstream pivot & melt
                 # (which insert the missing 0-value SF-133 lines)
                 # will work as expected (values used in the pivot
                 # index cannot be NULL).
                 # the "pad_to_length: 0" works around the fact
                 # that sometimes the incoming data for these columns
                 # is a single space and sometimes it is blank/NULL.
                 "beginning_period_of_availa": {"pad_to_length": 0},
                 "ending_period_of_availabil": {"pad_to_length": 0},
                 "availability_type_code": {"pad_to_length": 0},
                 "amount": {"strip_commas": True}}
            )

            # todo: find out how to handle dup rows (e.g., same tas/period/line number)
            # line numbers 2002 and 2012 are the only duped SF 133 report line numbers,
            # and they are not used by the validation rules, so for now
            # just remove them before loading our SF-133 table
            dupe_line_numbers = ['2002', '2102']
            data = data[~data.line.isin(dupe_line_numbers)]

            # add concatenated TAS field for sorting and splitting data
            data['tas'] = data.apply(lambda row: ''.join([
                row['allocation_transfer_agency'] if row['allocation_transfer_agency'] else '000',
                row['agency_identifier'] if row['agency_identifier'] else '000',
                row['beginning_period_of_availa'] if row['beginning_period_of_availa'].strip() else '0000',
                row['ending_period_of_availabil'] if row['ending_period_of_availabil'].strip() else '0000',
                row['availability_type_code'].strip() if row['availability_type_code'].strip() else ' ',
                row['main_account_code'] if row['main_account_code'] else '0000',
                row['sub_account_code'] if row['sub_account_code'] else '000'
            ]), axis=1)
            data['amount'] = data['amount'].astype(float)

            data = load_sf133.fill_blank_sf133_lines(data)

            # remove all lines not included in validation
            sf_133_validation_lines = [
                '1000', '1010', '1011', '1012', '1013', '1014', '1015', '1016', '1017', '1018', 
                '1019', '1020', '1021', '1022', '1023', '1024', '1025', '1026', '1027', '1028', 
                '1029', '1030', '1031', '1032', '1033', '1034', '1035', '1036', '1037', '1038', 
                '1039', '1040', '1041', '1042', '1160', '1180', '1260', '1280', '1340', '1440', 
                '1540', '1640', '1750', '1850', '1910', '2190', '2490', '2500'
            ]
            data = data[(data.line.isin(sf_133_validation_lines) & (data.amount!=0))]

            # create list of unique TAS
            data.sort_values(by=['tas'], inplace=True)
            data.set_index(keys=['tas'], drop=False, inplace=True)
            all_tas=data['tas'].unique().tolist()

            # iterate through TAS list
            for tas in all_tas:
                current_tas = data.loc[data.tas==tas]

                sum_one = 0
                validate_one = 0
                validate_one_lines = [
                    '1000', '1010', '1011', '1012', '1013', '1014', '1015', '1016', '1017', 
                    '1018', '1019', '1020', '1021', '1022', '1023', '1024', '1025', '1026', 
                    '1027', '1028', '1029', '1030', '1031', '1032', '1033', '1034', '1035', 
                    '1036', '1037', '1038', '1039', '1040', '1041', '1042', '1160', '1180', 
                    '1260', '1280', '1340', '1440', '1540', '1640', '1750', '1850'
                ]

                sum_two = 0
                validate_two = 0
                validate_two_lines = ['2190', '2490']
                for index, row in current_tas.iterrows():
                    if row.line in validate_one_lines:
                        sum_one += row.amount
                    elif row.line=='1910':
                        validate_one = float("{0:.2f}".format(row.amount))
                    elif row.line in validate_two_lines:
                        sum_two += row.amount
                    elif row.line=='2500':
                        validate_two = float("{0:.2f}".format(row.amount))
                sum_one = float("{0:.2f}".format(sum_one))
                sum_two = float("{0:.2f}".format(sum_two))

                validation_one = sum_one==validate_one
                validation_two = sum_two==validate_two
                validation_three = validate_one==validate_two

                tas_m = TAS_RE.match(tas)

                # initial validations fail
                if not (validation_one and validation_two) and (tas_m and tas_m.group('atc')!='X' and file_fiscal_year>int(tas_m.group('epoa'))):
                    if not validation_three:
                        failed_validations.append('{0},{1},{2},{3},{4},{5},{6},{7},1900!=2500,{8},{9}'.format(
                            file_index, tas_m.group('ata'), tas_m.group('aid'), 
                            tas_m.group('atc'), tas_m.group('bpoa'), tas_m.group('epoa'), 
                            tas_m.group('mac'), tas_m.group('sac'), validate_one, validate_two)
                        )
                else:
                    if not validation_one:
                        failed_validations.append('{0},{1},{2},{3},{4},{5},{6},{7},sum!=1910,{8},{9}'.format(
                            file_index, tas_m.group('ata'), tas_m.group('aid'), 
                            tas_m.group('atc'), tas_m.group('bpoa'), tas_m.group('epoa'), 
                            tas_m.group('mac'), tas_m.group('sac'), sum_one, validate_one)
                        )
                    if not validation_two:
                        failed_validations.append('{0},{1},{2},{3},{4},{5},{6},{7},sum!=2500,{8},{9}'.format(
                            file_index, tas_m.group('ata'), tas_m.group('aid'), 
                            tas_m.group('atc'), tas_m.group('bpoa'), tas_m.group('epoa'), 
                            tas_m.group('mac'), tas_m.group('sac'), sum_two, validate_two)
                        )

    assert len(failed_validations)==1, "\n".join(str(failure) for failure in failed_validations)
