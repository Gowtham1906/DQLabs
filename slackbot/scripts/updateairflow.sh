#!/bin/sh
cd temp
git clone https://${GITHUB_TOKEN}@github.com/DQLabs-Inc/DQLabs-Airflow.git
cd DQLabs-Airflow
cd infra/airflow/
docker build -t dqlabs-airflow:latest .
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 655275087384.dkr.ecr.us-east-1.amazonaws.com
docker tag dqlabs-airflow:latest 655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow:$1
docker push 655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow:$1

kubectl set image deployment/airflow-flower airflow-flower=655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow:$1 -n airflow
kubectl set image deployment/airflow-worker airflow-worker=655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow:$1 -n airflow
kubectl set image deployment/airflow-webserver airflow-webserver=655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow:$1 -n airflow
kubectl set image deployment/airflow-scheduler airflow-scheduler=655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow:$1 -n airflow
kubectl set image deployment/airflow-triggerer airflow-triggerer=655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow:$1 -n airflow
cd ../../../
rm -rf DQLabs-Airflow