FROM python:3.12

RUN apt-get -y update
RUN apt-get install -y gcc libpq-dev
RUN apt-get install -y postgresql-client
RUN apt-get install -y netcat-openbsd
RUN apt-get install -y libsqlite3-dev

RUN pip install unittest-xml-reporting setuptools==68.1.2

COPY requirements.txt /data-act/backend/requirements.txt
COPY server_requirements.txt /data-act/backend/server_requirements.txt

RUN pip install --upgrade pip==24.0
RUN pip install -r /data-act/backend/requirements.txt
# uwsgi fails to build its wheel on Alpine Linux without CPUCOUNT=1 - https://github.com/unbit/uwsgi/issues/1318
RUN CPUCOUNT=1 pip install -r /data-act/backend/server_requirements.txt

RUN opentelemetry-bootstrap -a install

ENV PYTHONPATH /data-act/backend
WORKDIR /data-act/backend

VOLUME /data-act/backend
ADD . /data-act/backend

CMD /bin/sh
