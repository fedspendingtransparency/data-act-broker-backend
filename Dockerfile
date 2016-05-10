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

ADD . /backend

RUN pip install -r backend/requirements.txt

CMD export PYTHONPATH="${PYTHONPATH}:/"

CMD /bin/sh