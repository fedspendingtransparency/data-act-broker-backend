FROM python:3.5

RUN apt-get -y update
RUN apt-get install -y postgresql-client

RUN pip install unittest-xml-reporting

COPY requirements.txt /data-act/backend/requirements.txt
COPY server_requirements.txt /data-act/backend/server_requirements.txt

RUN pip install -r /data-act/backend/requirements.txt
RUN pip install -r /data-act/backend/server_requirements.txt

ENV PYTHONPATH /data-act/backend
WORKDIR /data-act/backend

VOLUME /data-act/backend
ADD . /data-act/backend

CMD /bin/sh
