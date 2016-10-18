from csv import DictWriter

from dataactvalidator.scripts import loadTas


def import_tas(session, tmpdir, *rows):
    """Write the provided rows to a CSV, then read them in via pandas and
    clean them"""
    csv_file = tmpdir.join("all_tas_betc.csv")
    with open(str(csv_file), 'w') as f:
        writer = DictWriter(
            f, ['ATA', 'AID', 'A', 'BPOA', 'EPOA', 'MAIN', 'SUB']
        )
        writer.writeheader()
        for row in rows:
            data = {key: '' for key in writer.fieldnames}
            data.update(row)
            writer.writerow(data)

    return loadTas.cleanTas(str(csv_file))


def test_loadTas_multiple(database, tmpdir):
    """If we have two rows in the CSV, we should have two TASLookups"""
    results = import_tas(
        database.session, tmpdir,
        {'ATA': 'aaa', 'AID': 'bbb', 'A': 'ccc', 'BPOA': 'ddd', 'EPOA': 'eee',
         'MAIN': 'ffff', 'SUB': 'ggg'},
        {'ATA': '111', 'AID': '222', 'A': '333', 'BPOA': '444', 'EPOA': '555',
         'MAIN': '6666', 'SUB': '777'}
    )
    assert results['allocation_transfer_agency'].tolist() == ['aaa', '111']
    assert results['agency_identifier'].tolist() == ['bbb', '222']
    assert results['availability_type_code'].tolist() == ['ccc', '333']
    assert results['beginning_period_of_availability'].tolist() == [
        'ddd', '444']
    assert results['ending_period_of_availability'].tolist() == ['eee', '555']
    assert results['main_account_code'].tolist() == ['ffff', '6666']
    assert results['sub_account_code'].tolist() == ['ggg', '777']


def test_loadTas_space_nulls(database, tmpdir):
    results = import_tas(
        database.session, tmpdir, {'BPOA': '', 'EPOA': ' ', 'A': '   '})
    assert results['beginning_period_of_availability'][0] is None
    assert results['ending_period_of_availability'][0] is None
    assert results['availability_type_code'][0] is None
