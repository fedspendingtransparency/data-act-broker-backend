FROM fedora

RUN dnf install -y git \
        postgresql-devel \
        gcc \
        libffi-devel \
        python-devel \
        python-pip \
        pcre-devel \
        redhat-rpm-config

RUN pip install unittest-xml-reporting

ADD . /

RUN pip install -r requirements.txt

CMD export PYTHONPATH="${PYTHONPATH}:/"

CMD /bin/sh