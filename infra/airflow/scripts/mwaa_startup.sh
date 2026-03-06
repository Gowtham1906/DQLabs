#!/bin/sh

echo "Installing mssql odbc drivers"
export DEBIAN_FRONTEND=noninteractive
sudo yum update -y
sudo yum install -y curl
curl https://packages.microsoft.com/config/rhel/9/prod.repo | sudo tee /etc/yum.repos.d/mssql-release.repo
sudo yum update -y
sudo ACCEPT_EULA=Y yum install -y msodbcsql17
sudo ACCEPT_EULA=Y yum install -y mssql-tools
echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
source ~/.bashrc

#echo "Installing unix odbc modules"
sudo yum update -y
sudo yum install -y ant
sudo yum install -y krb5-devel
sudo yum install -y unixODBC-devel
sudo yum install -y wget
sudo yum clean all

echo "Add Denodo ODBC Driver Details"
sudo sh -c "echo '[DenodoODBCDriver]' >>/etc/odbcinst.ini"
sudo sh -c "echo 'Description=ODBC driver of Denodo' >> /etc/odbcinst.ini"
sudo sh -c "echo 'Driver=/usr/local/airflow/plugins/driver/odbc/denodo-vdp-odbcdriver-linux/lib/unixodbc_x64/denodoodbc.so' >> /etc/odbcinst.ini"
sudo sh -c "echo 'UsageCount=1' >> /etc/odbcinst.ini"

sudo yum install libxcrypt-compat -y

echo "################Installing Oracle##########################"

sudo touch /etc/yum.repos.d/mssql-release.repo
sudo chown $USER.root /etc/yum.repos.d/mssql-release.repo
sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc
sudo curl -o /etc/yum.repos.d/mssql-release.repo https://s3.amazonaws.com/dqlabs2.0/prod.repo
sudo yum check-update
sudo yum install -y unixODBC-devel
source ~/.bashrc
cd /tmp/
sudo yum install -y libaio.x86_64
sudo dnf install -y https://s3.amazonaws.com/dqlabs2.0/epel-release-latest-9.noarch.rpm
sudo dnf upgrade --nobest -y
sudo yum update --nobest -y
wget https://download.oracle.com/otn_software/linux/instantclient/217000/oracle-instantclient-basiclite-21.7.0.0.0-1.el8.x86_64.rpm
sudo dnf install -y oracle-instantclient*
echo /usr/lib/oracle/21/client64/lib/ | sudo tee /etc/ld.so.conf.d/oracle.conf && sudo chmod o+r /etc/ld.so.conf.d/oracle.conf
echo 'export ORACLE_HOME=/usr/lib/oracle/21/client64' | sudo tee /etc/profile.d/oracle.sh && sudo chmod o+r /etc/profile.d/oracle.sh
echo 'export ORACLE_DRIVER_PATH=/usr/lib/oracle/21/client64' | sudo tee /etc/profile.d/oracle.sh && sudo chmod o+r /etc/profile.d/oracle.sh
sudo yum install -y unixODBC

sudo yum groupinstall "Development Tools" -y
sudo yum -y install gcc gcc-c++ kernel-devel
sudo yum -y install python-devel libxslt-devel libffi-devel openssl-devel

sudo yum install gcc openssl-devel libffi-devel python-devel -y
sudo yum install postgresql-devel


###### Connectors agent Installation ########
echo "Installing Agent wheel file"

pip install https://dqlabs-agent.s3.us-east-1.amazonaws.com/dqlabs/prod/3X/3.11/linux/agent/dqlabs_agent-1.0.5-py3-none-any.whl


pip install apache-airflow-providers-jdbc==3.3.0
pip install apache-airflow-providers-microsoft-mssql==3.6.0
pip install apache-airflow-providers-postgres==5.13.0

pip uninstall apache-airflow-providers-fab -y
pip uninstall apache-airflow-providers-smtp -y
pip uninstall apache-airflow-providers-common-compat -y
pip uninstall apache-airflow -y
pip install apache-airflow==2.8.1

echo "Installing reporting package"
pip install selenium==4.21.0
mkdir -p /tmp/chrome_install && cd /tmp/chrome_install && wget https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm && sudo dnf install -y ./google-chrome-stable_current_x86_64.rpm && cd ~ && rm -rf /tmp/chrome_install

pip install pymssql==2.3.0

## Atlan changes
pip install python-dateutil>=2.9.0
pip install pyatlan==6.2.0

## Semantics module packages
pip install typing-extensions==4.14.1 --force-reinstall
pip install sentence-transformers==4.0.1 --extra-index-url https://download.pytorch.org/whl/cpu
pip install fuzzywuzzy==0.18.0 --extra-index-url https://download.pytorch.org/whl/cpu
pip install RapidFuzz==3.12.2 --extra-index-url https://download.pytorch.org/whl/cpu
pip install gensim==4.3.3 --extra-index-url https://download.pytorch.org/whl/cpu
pip install opencv-python-headless --extra-index-url https://download.pytorch.org/whl/cpu
pip install torch==2.5.0 --extra-index-url https://download.pytorch.org/whl/cpu
pip install transformers>=4.50.0 --extra-index-url https://download.pytorch.org/whl/cpu
pip install typing-extensions==4.14.1 --force-reinstall
pip freeze
#pip install --upgrade typing-extensions

echo "MWAA script execution completed successfully with all packages installed"