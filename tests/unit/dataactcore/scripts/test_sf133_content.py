import re
import os

from dataactcore.config import CONFIG_BROKER
from dataactcore.scripts.pipeline import load_sf133

SF_RE = re.compile(r'sf_133_(?P<year>\d{4})_(?P<period>\d{2})\.csv')
line_sums = {}


def sum_range(tas, start, end, target, join_array, failed_validations, tas_str, extra_line=None):
    """ Rule: sum of lines [start] through [end] = [target] """
    lines_start_to_end = tas[tas.line.isin(list(map(str, range(start, end + 1))))]

    line_target = tas[tas.line == str(target)]
    sum_lines_start_to_end = float("{0:.2f}".format(lines_start_to_end['amount'].astype(float).sum()))

    if extra_line:
        line = tas[tas.line.isin([str(extra_line)])]
        line_amount = float("{0:.2f}".format(line['amount'].astype(float).sum()))
        sum_lines_start_to_end += line_amount

    line_target_amount = float("{0:.2f}".format(line_target['amount'].astype(float).sum()))
    if line_target_amount != sum_lines_start_to_end:
        error_message = 'Sum of lines {start} through {end} != {target}'.format(start=start, end=end, target=target)
        if extra_line:
            error_message = 'Sum of lines {start} through {end} + {extra_line} != {target}'.\
                format(start=start, end=end, extra_line=extra_line, target=target)
        failed_validations.append(','.join(join_array + [error_message, "{0:.2f}".format(line_target_amount),
                                                         "{0:.2f}".format(sum_lines_start_to_end)]))
    sum_key = "sum_lines_{start}_through_{end}".format(start=start, end=end)
    sum_dict = {sum_key: sum_lines_start_to_end, target: line_target_amount}

    if tas_str not in line_sums:
        line_sums[tas_str] = sum_dict
    else:
        line_sums[tas_str].update(sum_dict)


def sum_list(tas_str, line_list):
    line_amount_list = [line_sums[tas_str][key] for key in line_list if key in line_sums[tas_str]]
    return "{:.2f}".format(sum(line_amount_list))


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

            # sort data by unique TAS
            data.sort_values(by=['tas'], inplace=True)
            data.set_index(keys=['tas'], drop=False, inplace=True)

            # iterate through data by TAS
            for _, tas in data.groupby(['tas']):
                current_tas = tas.iloc[0]
                tas_list = [current_tas['agency_identifier'], current_tas['allocation_transfer_agency'],
                            current_tas['availability_type_code'], current_tas['beginning_period_of_availa'],
                            current_tas['ending_period_of_availabil'], current_tas['main_account_code'],
                            current_tas['sub_account_code']]
                join_array = [file_index] + tas_list
                tas_str = ''.join(tas_list)

                # Rule: sum of lines 1000 through 1042 = 1050
                sum_range(tas=tas, start=1000, end=1042, target=1050, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                # Rule: sum of lines 1100 through 1153 = 1160
                sum_range(tas=tas, start=1100, end=1153, target=1160, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                # Rule: sum of lines 1170 through 1176 = 1180
                sum_range(tas=tas, start=1170, end=1176, target=1180, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                # Rule: sum of lines 1200 through 1252 = 1260
                sum_range(tas=tas, start=1200, end=1252, target=1260, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                # Rule: sum of lines 1270 through 1276 = 1280
                sum_range(tas=tas, start=1270, end=1276, target=1280, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                # Rule: sum of lines 1300 through 1330 = 1340
                sum_range(tas=tas, start=1300, end=1330, target=1340, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                # Rule: sum of lines 1400 through 1430 = 1440
                sum_range(tas=tas, start=1400, end=1430, target=1440, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                # Rule: sum of lines 1500 through 1531 = 1540
                sum_range(tas=tas, start=1500, end=1531, target=1540, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                # Rule: sum of lines 1600 through 1631 = 1640
                sum_range(tas=tas, start=1600, end=1631, target=1640, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                # Rule: sum of lines 1700 through 1742 = 1750
                sum_range(tas=tas, start=1700, end=1742, target=1750, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                # Rule: sum of lines 1800 through 1842 = 1850
                sum_range(tas=tas, start=1800, end=1842, target=1850, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                line_1900_amount = "{:.2f}".format(tas[tas.line == '1900'].amount.astype(float).sum())
                line_1910_amount = "{:.2f}".format(tas[tas.line == '1910'].amount.astype(float).sum())
                # Rule: 1160 + 1180 + 1260 + 1280 + 1340 + 1440 + 1540 + 1640 + 1750 + 1850 = 1900
                line_list = [1160, 1180, 1260, 1280, 1340, 1440, 1540, 1640, 1750, 1850]
                line_amount_list_sum = sum_list(tas_str, line_list)

                if line_1900_amount != line_amount_list_sum:
                    failed_validations.append(','.join(join_array + ['1160 + 1180 + 1260 + 1280 + 1340 + 1440 + 1540 + '
                                                                     '1640 + 1750 + 1850 != 1900',
                                                                     line_1900_amount,
                                                                     line_amount_list_sum]))

                # Rule: 1050 + 1900 = 1910
                line_amount_list_sum = "{:.2f}".format(float(sum_list(tas_str, [1050])) + float(line_1900_amount))
                if line_1910_amount != line_amount_list_sum:
                    failed_validations.append(','.join(join_array + ['1050 + 1900 != 1910',
                                                                     line_1910_amount,
                                                                     line_amount_list_sum]))

                # Rule: sum of lines 1100 through 1153 +sum of lines 1170 through 1176 +sum of lines 1200 through 1252
                # +sum of lines 1270 through 1276 +sum of lines 1300 through 1330 +sum of lines 1400 through 1430 +sum
                # of lines 1500 through 1531 +sum of lines 1600 through 1631 +sum of lines 1700 through 1742 +sum of
                # lines 1800 through 1842 = 1900
                key_list = ['sum_lines_1100_through_1153', 'sum_lines_1170_through_1176', 'sum_lines_1200_through_1252',
                            'sum_lines_1270_through_1276', 'sum_lines_1300_through_1330', 'sum_lines_1400_through_1430',
                            'sum_lines_1500_through_1531', 'sum_lines_1600_through_1631', 'sum_lines_1700_through_1742',
                            'sum_lines_1800_through_1842']
                key_list_sum = sum_list(tas_str, key_list)
                if line_1900_amount != key_list_sum:
                    failed_validations.append(','.join(join_array + ['Sum of the sum of lines != 1900',
                                                                     line_1900_amount,
                                                                     key_list_sum]))

                # Rule: sum of lines 1000 through 1042 +sum of lines 1100 through 1153 +sum of lines 1170 through 1176
                # +sum of lines 1200 through 1252 +sum of lines 1270 through 1276 +sum of lines 1300 through 1330 +sum
                # of lines 1400 through 1430 +sum of lines 1500 through 1531 +sum of lines 1600 through 1631 +sum of
                # lines 1700 through 1742 +sum of lines 1800 through 1842 = 1910
                key_list = ['sum_lines_1000_through_1042', 'sum_lines_1100_through_1153', 'sum_lines_1170_through_1176',
                            'sum_lines_1200_through_1252', 'sum_lines_1270_through_1276', 'sum_lines_1300_through_1330',
                            'sum_lines_1400_through_1430', 'sum_lines_1500_through_1531', 'sum_lines_1600_through_1631',
                            'sum_lines_1700_through_1742', 'sum_lines_1800_through_1842']
                key_list_sum = sum_list(tas_str, key_list)
                if line_1910_amount != key_list_sum:
                    failed_validations.append(','.join(join_array + ['Sum of the sum of lines != 1910',
                                                                     line_1910_amount,
                                                                     key_list_sum]))

                # Turning this rule off until it is deemed necessary
                #
                # # Rule: sum of lines 2001 through 2003 = 2004
                # sum_range(tas=tas, start=2001, end=2003, target=2004, join_array=join_array,
                #           failed_validations=failed_validations, tas_str=tas_str)
                #
                # # Rule: sum of lines 2101 through 2103 = 2104
                # sum_range(tas=tas, start=2101, end=2103, target=2104, join_array=join_array,
                #           failed_validations=failed_validations, tas_str=tas_str)

                # Rule: 2004 + 2104 = 2190
                line_2004 = tas[tas.line.isin(['2004'])]
                line_2004_amount = float("{0:.2f}".format(line_2004['amount'].astype(float).sum()))

                line_2104 = tas[tas.line.isin(['2104'])]
                line_2104_amount = float("{0:.2f}".format(line_2104['amount'].astype(float).sum()))

                line_2190_amount = "{:.2f}".format(tas[tas.line == '2190'].amount.astype(float).sum())
                line_amount_sum = "{:.2f}".format(line_2004_amount + line_2104_amount)

                if line_2190_amount != line_amount_sum:
                    failed_validations.append(','.join(join_array + ['2004 + 2104 != 2190',
                                                                     line_2190_amount,
                                                                     line_amount_sum]))

                # Rule: 2170 + 2180 = 2190
                line_2170_amount = "{:.2f}".format(tas[tas.line == '2170'].amount.astype(float).sum())
                line_2180_amount = "{:.2f}".format(tas[tas.line == '2180'].amount.astype(float).sum())
                line_amount_sum = "{:.2f}".format(float(line_2170_amount) + float(line_2180_amount))

                if line_2190_amount != line_amount_sum:
                    failed_validations.append(','.join(join_array + ['2170 + 2180 != 2190',
                                                                     line_2190_amount,
                                                                     line_amount_sum]))

                # Rule: 2201 + 2202 + 2203 + 2301 + 2302 + 2303 + 2401 + 2402 + 2403 + 2413 = 2490
                line_2490_amount = "{:.2f}".format(tas[tas.line == '2490'].amount.astype(float).sum())

                lines_2201_to_2203 = tas[tas.line.isin(list(map(str, range(2201, 2204))))]
                sum_lines = float("{0:.2f}".format(lines_2201_to_2203['amount'].astype(float).sum()))

                lines_2301_to_2303 = tas[tas.line.isin(list(map(str, range(2301, 2304))))]
                sum_lines += float("{0:.2f}".format(lines_2301_to_2303['amount'].astype(float).sum()))

                lines_2401_to_2403 = tas[tas.line.isin(list(map(str, range(2401, 2404))))]
                sum_lines += float("{0:.2f}".format(lines_2401_to_2403['amount'].astype(float).sum()))

                line_2413 = tas[tas.line.isin(['2413'])]
                line_2143_amount = float("{0:.2f}".format(line_2413['amount'].astype(float).sum()))
                sum_lines += line_2143_amount

                sum_lines = "{:.2f}".format(sum_lines)

                if line_2490_amount != sum_lines:
                    failed_validations.append(','.join(join_array + ['2201 + 2202 + 2203 + 2301 + 2302 + 2303 + 2401 + '
                                                                     '2402 + 2403 + 2413 = 2490', line_2490_amount,
                                                                     sum_lines]))

                # Rule: 2412 + 2413 = 2490
                sum_range(tas=tas, start=2412, end=2413, target=2490, join_array=join_array,
                          failed_validations=failed_validations, tas_str=tas_str)

                # Turning this rule off until it is deemed necessary
                #
                # # Rule: (sum of lines 2001 through 2403) + 2413 = 2500
                # sum_range(tas=tas, start=2001, end=2403, target=2500, join_array=join_array,
                #           failed_validations=failed_validations, tas_str=tas_str, extra_line=2413)

                # Rule: 1910 = 2500
                line_amount = "{:.2f}".format(tas[tas.line == '2500'].amount.astype(float).sum())
                if line_1910_amount != line_amount:
                    failed_validations.append(','.join(join_array + ['1910 != 2500',
                                                                     line_1910_amount,
                                                                     line_amount]))

    assert len(failed_validations) == 1, "\n".join(str(failure) for failure in failed_validations)
