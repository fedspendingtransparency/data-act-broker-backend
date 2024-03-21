FROM python:3.10.13

RUN apt-get -y update
RUN apt-get install -y gcc libpq-dev
RUN apt-get install -y postgresql-client
RUN apt-get install -y netcat-openbsd
RUN apt-get install -y libsqlite3-dev
RUN apt-get install -y python3-dev python3-pip
RUN apt-get install -y build-essential python3-dev python3-pip python3-venv python3-wheel

COPY requirements.txt /data-act/backend/requirements.txt
COPY server_requirements.txt /data-act/backend/server_requirements.txt

ENV CPUCOUNT 1
RUN pip3 install --upgrade pip
RUN pip3 install unittest-xml-reporting setuptools==58.2.0 wheel
RUN pip3 install -r /data-act/backend/requirements.txt
RUN pip3 install -r /data-act/backend/server_requirements.txt

ENV PYTHONPATH /data-act/backend
WORKDIR /data-act/backend

VOLUME /data-act/backend
ADD . /data-act/backend

CMD /bin/sh
