if [ -z $1 ] ; then
  echo "usage $0 airflow tag {1.0 ......}"
  exit 11
fi
# docker compose down
docker-compose down
cd airflow
docker build -t dqlabs-airflow:"${1}" .
cd ../
docker login
export airflow_tag=dqlabs-airflow:"${1}"
export dag_service_tag=dqlabs-dagservice:"${1}"
if [ -z "$2" ]
then
  export dqlabs_server_tag=dqlabs-server:"${1}"
else
  export dqlabs_server_tag=dqlabs-server:"${2}"
fi
docker network connect airflow airflow-webserver
# docker compose up -d
docker-compose up -d

