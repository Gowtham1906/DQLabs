FROM --platform=linux/amd64 apache/airflow:2.8.1-python3.11

USER root

#ODBC Driver for MSSQl
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update && \
    apt-get install -yq --no-install-recommends apt-utils curl apt-transport-https gnupg && \
    echo "needrestart needrestart/enable-stamp boolean false" | debconf-set-selections && \
    echo "needrestart needrestart/restart-services string none" | debconf-set-selections && \
    \
    # --- Fix MariaDB repository issue (use official mirror) ---
    curl -LsS https://mariadb.org/mariadb_release_signing_key.asc | gpg --dearmor | tee /etc/apt/trusted.gpg.d/mariadb.gpg > /dev/null && \
    echo "deb [arch=amd64,arm64,ppc64el] https://mirror.mariadb.org/repo/10.11/debian bookworm main" > /etc/apt/sources.list.d/mariadb.list && \
    \
    # --- Add Microsoft SQL Server repo ---
    curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc && \
    curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | tee /etc/apt/sources.list.d/mssql-release.list && \
    \
    # --- Update and install required packages ---
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools && \
    \
    # --- Add MSSQL tools to PATH ---
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

# Install OpenJDK
RUN apt-get -f install && apt-get autoremove && apt-get clean;
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get install -y unixodbc unixodbc-dev freetds-dev freetds-bin tdsodbc libssl-dev postgresql-server-dev-9.3 && \
    apt-get clean;


# Install oracle client
RUN apt-get install -y wget alien libaio1
RUN wget https://download.oracle.com/otn_software/linux/instantclient/217000/oracle-instantclient-basiclite-21.7.0.0.0-1.el8.x86_64.rpm
RUN alien -i oracle-instantclient*
RUN ls /usr/lib/oracle
RUN echo /usr/lib/oracle/21/client64/lib/ | sudo tee /etc/ld.so.conf.d/oracle.conf && sudo chmod o+r /etc/ld.so.conf.d/oracle.conf
RUN echo 'export ORACLE_HOME=/usr/lib/oracle/21/client64' | sudo tee /etc/profile.d/oracle.sh && sudo chmod o+r /etc/profile.d/oracle.sh

RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt update
RUN apt install -y ./google-chrome-stable_current_amd64.deb


# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME
COPY jars/* /usr/lib/jars/

#Denodo ODBC Driver copy and install and create entry in odbcinst.ini file
RUN tar -xzf /usr/lib/jars/denodo-vdp-odbcdriver-linux.tar.gz --directory /usr/lib/jars
RUN echo '[DenodoODBCDriver]' >>/etc/odbcinst.ini
RUN echo 'Description=ODBC driver of Denodo' >> /etc/odbcinst.ini
RUN echo 'Driver=/usr/lib/jars/denodo-vdp-odbcdriver-linux/lib/unixodbc_x64/denodoodbc.so' >> /etc/odbcinst.ini
RUN echo 'UsageCount=1' >> /etc/odbcinst.ini

USER airflow
COPY airflow-eks.cfg airflow.cfg
COPY dags /opt/airflow/dags
RUN pip install -r requirements_mwaa_2.8.1.txt
RUN python dags/setup.py bdist_wheel
RUN pip install dist/*.whl --force-reinstall
# Install agent whl distributions

RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/agent/dqlabs_agent-1.0.2-py3-none-any.whl

RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/airflow/dqlabs_connectors_airflow-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/oracle/dqlabs_connectors_oracle-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/databricks/dqlabs_connectors_databricks-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/dbt/dqlabs_connectors_dbt-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/mssql/dqlabs_connectors_mssql-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/saphana/dqlabs_connectors_saphana-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/synapse/dqlabs_connectors_synapse-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/snowflake/dqlabs_connectors_snowflake-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/athena/dqlabs_connectors_athena-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/bigquery/dqlabs_connectors_bigquery-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/fivetran/dqlabs_connectors_fivetran-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/postgresql/dqlabs_connectors_postgresql-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/powerbi/dqlabs_connectors_powerbi-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/redshift_spectrum/dqlabs_connectors_redshift_spectrum-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/redshift/dqlabs_connectors_redshift-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/s3select/dqlabs_connectors_s3select-1.0.0-py3-none-any.whl

RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/db2/dqlabs_connectors_db2-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/db2ibm/dqlabs_connectors_db2ibm-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/teradata/dqlabs_connectors_teradata-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/adf/dqlabs_connectors_adf-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/adls/dqlabs_connectors_adls-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/denodo/dqlabs_connectors_denodo-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/mysql/dqlabs_connectors_mysql-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/hive/dqlabs_connectors_hive-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/emr_spark/dqlabs_connectors_emr_spark-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/talend/dqlabs_connectors_talend-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/tableau/dqlabs_connectors_tableau-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/coalesce_pipeline/dqlabs_connectors_coalesce_pipeline-1.0.0-py3-none-any.whl
RUN pip install https://dqlabs-agent.s3.amazonaws.com/distributions/3X/3.11/linux/connectors/sapecc/dqlabs_connectors_sapecc-1.0.0-py3-none-any.whl

#RUN pip install --upgrade cryptography
#RUN pip install --upgrade pyopenssl
#RUN pip uninstall apache-airflow-providers-fab -y
RUN pip install --upgrade typing-extensions
RUN rm -rf /opt/airflow/dags/setup.py
RUN rm -rf /opt/airflow/dags/clear_airflow_storage.py

USER airflow

