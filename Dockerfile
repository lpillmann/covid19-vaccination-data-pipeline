FROM apache/airflow

ENV DEBIAN_FRONTEND noninteractive

USER root

# Install dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    make \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    wget \
    curl \
    llvm \
    libncurses5-dev \
    xz-utils \
    tk-dev \
    libxml2-dev \
    libxmlsec1-dev \
    git \
    ca-certificates \
    libffi-dev \
    jq \
  && apt-get clean autoclean \
  && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/* \
  && rm -f /var/cache/apt/archives/*.deb

USER airflow

# Install pyenv
RUN mkdir /home/airflow/.pyenv
RUN git clone https://github.com/pyenv/pyenv /home/airflow/.pyenv

# Install Python 3.8
RUN set -ex \
    && /home/airflow/.pyenv/bin/pyenv install 3.8.7 \
    && /home/airflow/.pyenv/versions/3.8.7/bin/python -m pip install --upgrade pip
ENV PATH /home/airflow/.pyenv/versions/3.8.7/bin:${PATH}

# Install virtualenvs
ENV PIP_USER false

RUN /home/airflow/.pyenv/versions/3.8.7/bin/python -m venv venvs/tap-opendatasus \
  && venvs/tap-opendatasus/bin/python -m pip install --no-cache-dir git+https://github.com/lpillmann/tap-opendatasus.git certifi

RUN /home/airflow/.pyenv/versions/3.8.7/bin/python -m venv venvs/target-s3-csv \
  && venvs/target-s3-csv/bin/python -m pip install --no-cache-dir git+https://github.com/lpillmann/pipelinewise-target-s3-csv.git

RUN /home/airflow/.pyenv/versions/3.8.7/bin/python -m venv venvs/awscli \
  && venvs/awscli/bin/python -m pip install --no-cache-dir awscli
