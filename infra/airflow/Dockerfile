FROM --platform=linux/amd64 apache/airflow:2.8.1-python3.11

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
# RUN apt-get -f install && apt-get autoremove && apt-get -y dist-upgrade && apt-get clean;
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

#Copy jar file
COPY jars/* /usr/lib/jars/

#Denodo ODBC Driver copy and install and create entry in odbcinst.ini file
RUN tar -xzf /usr/lib/jars/denodo-vdp-odbcdriver-linux.tar.gz --directory /usr/lib/jars
RUN echo '[DenodoODBCDriver]' >>/etc/odbcinst.ini
RUN echo 'Description=ODBC driver of Denodo' >> /etc/odbcinst.ini
RUN echo 'Driver=/usr/lib/jars/denodo-vdp-odbcdriver-linux/lib/unixodbc_x64/denodoodbc.so' >> /etc/odbcinst.ini
RUN echo 'UsageCount=1' >> /etc/odbcinst.ini

USER airflow

COPY dags/requirements.txt requirements.txt
COPY airflow.cfg airflow.cfg
COPY dags /opt/airflow/dags
RUN pip install -r requirements.txt

COPY dags/agent_requirements_dev.txt agent_requirements.txt
RUN pip install -r agent_requirements.txt

# Install agent whl distributions
COPY agent_distributions /opt/airflow/agent_distributions
RUN pip install /opt/airflow/agent_distributions/*.whl
RUN pip install --upgrade typing-extensions
RUN pip install --upgrade cryptography
RUN pip install --upgrade pyopenssl