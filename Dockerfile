FROM python:3.12

RUN apt-get -y update
RUN apt-get install -y libpq-dev
RUN apt-get install -y postgresql-client
RUN apt-get install -y netcat-openbsd
RUN apt-get install -y libsqlite3-dev
RUN apt-get install -y build-essential

# Install Java
#RUN wget -qO - https://apt.corretto.aws/corretto.key | gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg && \
#    echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list
#RUN apt update && \
#    apt install -y java-1.8.0-amazon-corretto-jdk
#ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto
#
#ARG HADOOP_VERSION=3.3.4
#ARG SPARK_VERSION=3.5.0

#WORKDIR /usr/local
#
#RUN wget --quiet https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
#    && tar xzf hadoop-${HADOOP_VERSION}.tar.gz \
#    && ln -sfn /usr/local/hadoop-${HADOOP_VERSION} /usr/local/hadoop \
#    && wget --quiet https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
#    && tar xzf spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
#    && ln -sfn /usr/local/spark-${SPARK_VERSION}-bin-without-hadoop /usr/local/spark \
#    && echo "Installed $(/usr/local/hadoop/bin/hadoop version)"
#ENV HADOOP_HOME=/usr/local/hadoop
#ENV SPARK_HOME=/usr/local/spark
## Cannot set ENV var = command-result, [i.e. doing: ENV SPARK_DIST_CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath)], so interpolating the hadoop classpath the long way
#ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
#ENV PATH=${SPARK_HOME}/bin:${HADOOP_HOME}/bin:${JAVA_HOME}/bin:${PATH}
#RUN echo "Installed Spark" && echo "$(${SPARK_HOME}/bin/pyspark --version)"

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
