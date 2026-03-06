FROM --platform=linux/amd64 apache/airflow:2.8.1-python3.11

ARG DQLABS_ENV
USER root

#ODBC Driver for MSSQl 
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update && \
    apt-get install -yq --no-install-recommends apt-utils && \
    apt-get install -y curl apt-transport-https && \
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc  && \
    curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc


# Install OpenJDK
RUN apt-get -f install && apt-get autoremove && apt-get clean;
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get install -y unixodbc unixodbc-dev freetds-dev freetds-bin tdsodbc libssl-dev postgresql-server-dev-9.3 && \
    apt-get clean;
# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

# Install oracle client
RUN apt-get install -y wget alien libaio1
RUN wget https://download.oracle.com/otn_software/linux/instantclient/217000/oracle-instantclient-basiclite-21.7.0.0.0-1.el8.x86_64.rpm
RUN alien -i oracle-instantclient*
RUN ls /usr/lib/oracle
RUN echo /usr/lib/oracle/21/client64/lib/ | sudo tee /etc/ld.so.conf.d/oracle.conf && sudo chmod o+r /etc/ld.so.conf.d/oracle.conf
RUN echo 'export ORACLE_HOME=/usr/lib/oracle/21/client64' | sudo tee /etc/profile.d/oracle.sh && sudo chmod o+r /etc/profile.d/oracle.sh

# Install dependencies for Chrome
RUN apt-get update && \
    apt-get install -y wget unzip gnupg telnet curl && \
    wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" \
        > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY airflow-eks.cfg airflow.cfg
COPY dags /opt/airflow/dags
RUN pip install pkutils
WORKDIR /opt/airflow/dags

###### Connectors agent Installation ########
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/agent/dqlabs_agent-1.0.8-py3-none-any.whl --force-reinstall

RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/airflow/dqlabs_connectors_airflow-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/oracle/dqlabs_connectors_oracle-1.0.1-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/databricks/dqlabs_connectors_databricks-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/dbt/dqlabs_connectors_dbt-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/mssql/dqlabs_connectors_mssql-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/saphana/dqlabs_connectors_saphana-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/synapse/dqlabs_connectors_synapse-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/snowflake/dqlabs_connectors_snowflake-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/athena/dqlabs_connectors_athena-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/bigquery/dqlabs_connectors_bigquery-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/fivetran/dqlabs_connectors_fivetran-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/postgresql/dqlabs_connectors_postgresql-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/powerbi/dqlabs_connectors_powerbi-1.0.1-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/redshift_spectrum/dqlabs_connectors_redshift_spectrum-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/redshift/dqlabs_connectors_redshift-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/s3select/dqlabs_connectors_s3select-1.0.0-py3-none-any.whl --force-reinstall

RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/db2/dqlabs_connectors_db2-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/db2ibm/dqlabs_connectors_db2ibm-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/teradata/dqlabs_connectors_teradata-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/adf/dqlabs_connectors_adf-1.0.2-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/adls/dqlabs_connectors_adls-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/denodo/dqlabs_connectors_denodo-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/mysql/dqlabs_connectors_mysql-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/hive/dqlabs_connectors_hive-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/emr_spark/dqlabs_connectors_emr_spark-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/talend/dqlabs_connectors_talend-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/tableau/dqlabs_connectors_tableau-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/wherescape/dqlabs_connectors_wherescape-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/alloydb/dqlabs_connectors_alloydb-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/mulesoft/dqlabs_connectors_mulesoft-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/file/dqlabs_connectors_file-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/salesforce/dqlabs_connectors_salesforce-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/spark/dqlabs_connectors_spark-1.0.1-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/salesforce_marketing/dqlabs_connectors_salesforce_marketing-1.0.1-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/coalesce_pipeline/dqlabs_connectors_coalesce_pipeline-1.0.1-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/sapecc/dqlabs_connectors_sapecc-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/sigma/dqlabs_connectors_sigma-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/s3/dqlabs_connectors_s3-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/salesforce_data_cloud/dqlabs_connectors_salesforce_data_cloud-1.0.3-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/airbyte/dqlabs_connectors_airbyte-1.0.0-py3-none-any.whl --force-reinstall
RUN pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/${DQLABS_ENV}/3X/3.11/linux/connectors/mongo/dqlabs_connectors_mongo-1.0.0-py3-none-any.whl


RUN python setup.py bdist_wheel
RUN pip install dist/*.whl --force-reinstall
RUN pip install -r airfloweks_requirements_2.8.1.txt
