# migrate data using pg_dump and pg_restore
# data copied from tables:
# error_data:
#   error_metadata
#   file
# job_tracker:
#   job
#   submission
#   job_dependency
# user_manager;
#   users
#   email_token
# validator:
#   appropriation
#   award_financial
#   award_financial_assistance
#   object_class_program_activity

# run on command line: python migrateDataBroker.py

from dataactcore.config import CONFIG_DB
import subprocess

c = 'postgresql://{}:"{}"@{}/'.format(
    CONFIG_DB['username'], CONFIG_DB['password'], CONFIG_DB['host'])
target = '{}data_broker'.format(c)

# error_data
db = 'error_data'
source = '{}{}'.format(c, db)
print('migrating {}'.format(db))
cmd = 'pg_dump -d {} -t error_metadata -t file --data-only --format=c | ' \
    'pg_restore -d {} --data-only --single-transaction'.format(source, target)
p = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE)
print('return code = {}\n'.format(p))

# job_tracker
db = 'job_tracker'
source = '{}{}'.format(c, db)
print('migrating {}'.format(db))
cmd = 'pg_dump -d {} -t job_dependency -t job -t submission --data-only --format=c | ' \
    'pg_restore -d {} --data-only --single-transaction'.format(source, target)
p = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE)
print('return code = {}\n'.format(p))

# user_manager
db = 'user_manager'
source = '{}{}'.format(c, db)
print('migrating {}'.format(db))
cmd = 'pg_dump -d {} -t users -t email_token --data-only --format=c | ' \
    'pg_restore -d {} --data-only --single-transaction'.format(source, target)
p = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE)
print('return code = {}\n'.format(p))

# validation - these tables are larger, so do individually
db = 'validation'
source = '{}{}'.format(c, db)
tables = ['appropriation', 'object_class_program_activity',
          'award_financial', 'award_financial_assistance']

for t in tables:
    print('migrating {}: {}'.format(db, t))
    # old db still has valid_record column; drop it
    cmd = 'psql {} -c "ALTER TABLE {} DROP COLUMN IF EXISTS valid_record"'.format(source, t)
    p = subprocess.call(cmd, shell=True)
    cmd = 'pg_dump -d {} -t {} --data-only --format=c | ' \
        'pg_restore -d {} --data-only --single-transaction'.format(source, t, target)
    p = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE)
    print('return code = {}\n'.format(p))


