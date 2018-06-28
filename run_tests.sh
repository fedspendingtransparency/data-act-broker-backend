set -e
# Wait for postgres container
sleep 15
>&2 echo \"Postgres is up - executing command\"
# Run testing commands
cd /data-act/backend/dataactcore
alembic upgrade head
cd ..
flake8
py.test --cov=. --cov-report xml:tests/coverage.xml --junitxml=tests/test-results.xml
