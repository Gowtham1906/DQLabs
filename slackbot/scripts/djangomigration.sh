cd scripts/djangomigration/Dqlabs-Server-2.0
apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get install -y unixodbc-dev &&  \
    apt-get install -y unixodbc && \
    apt-get install -y libpq-dev && \
    apt-get clean;
git pull
cp ../dev1.env src/dev.env
cp ../dev1.env src/.env
cp ../requirements.txt src/requirements.txt
cd src
export env=dev
pip3.9 install -r requirements.txt
pip3.9 uninstall psycopg2
pip3.9 install psycopg2-binary
pip3.9 install environ
pip3.9 install urllib3
cd core
find . -path "*/migrations/*.py" -not -name "__init__.py" -delete && find . -path "*/migrations/*.pyc"  -delete
cd ../
python3.9 manage.py init_server
