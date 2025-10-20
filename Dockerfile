FROM python:3.12

RUN apt-get -y update
RUN apt-get install -y libpq-dev
RUN apt-get install -y postgresql-client
RUN apt-get install -y netcat-openbsd
RUN apt-get install -y libsqlite3-dev
RUN apt-get install -y build-essential

# Install Java
RUN wget -qO - https://apt.corretto.aws/corretto.key | gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list
RUN apt update && \
    apt install -y java-1.8.0-amazon-corretto-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto

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
