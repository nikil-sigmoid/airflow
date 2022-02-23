FROM python:3.8.5

# Update and install system packages
RUN apt-get update -y && apt-get install jq -y && \
  apt-get install --no-install-recommends -y -q \
  git libpq-dev python-dev && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && apt-get install coreutils && \
   apt-get install jq -y 

# RUN curl -sSL https://sdk.cloud.google.com | bash 
# ENV PATH $PATH:/root/google-cloud-sdk/bin

# Install DBT
RUN pip install -U pip
RUN pip3 install dbt-bigquery==0.21.1
RUN pip install gsutil

# Download DBT_parse_package
RUN pip install dbt_log_parser==0.2.1

COPY . ./

WORKDIR /project_dir

ENTRYPOINT ["bash", "./run_dbt_seed.sh"]