# Airflow

## Description :

Run dags in orchestration via airflow.

## Run airflow :

sh start-airflow.sh <tag_id>

## Folder structure :

- dags: Please place all the python dags here
- airflow : Houses the docker component of building the custom spark airflow image
- resources: Place input data to the dags here
- docs/screenshots : Necessary connection success run screenshots

## Connections :

As of now airflow connections to spark snowflake are created in the airflow UI.
Please reach out to @nikhil or @gobi for snowflake credentials

## Spin up with DQLabs-Server
- Build the dqlabs server image first and get the imageId
- Pass that image id as second param to sh start-airflow.sh command.
- Pull Dqlabs-Server repo
```
cd Dqlabs-Server-2.0/src
docker build -t dqlabs-server:1.0 .
cd ../../DQlabs-Airflow/infra
sh start-airflow.sh 1.0 dqlabs-server:1.0

```



## Current Setup

- Airflow UI : http://3.83.250.80:8080/
- Credentials : airflow/airflow

## How to connect to ec2

`ssh -i dqlabs-ec2.pem ubuntu@ec2-3-83-250-80.compute-1.amazonaws.com`
- Please find the pem file in `https://dqlabsinc.slack.com/archives/C03EJ7T0M32/p1652121582839619`

## Machine spec:
- EC2 c8xlarge will downgrade/upgrade based on usage

## Dynamic dags
- Dags are created dynamically based on the state of `daginfo` table in `DQLABS` db in `postgres`.
- When there is a new entry in to `daginfo` with the corresponding logic class => this is the name of the python file. In our case it could be extract_properties.py or something, the dags corresponding to the logic class state is modified.
