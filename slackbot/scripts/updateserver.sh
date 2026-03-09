#!/bin/sh
cd temp
rm -rf Dqlabs-Server-2.0
git clone https://${GITHUB_TOKEN}@github.com/DQLabs-Inc/Dqlabs-Server-2.0.git
cd Dqlabs-Server-2.0
docker build -t dqlabs-server:latest .
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 655275087384.dkr.ecr.us-east-1.amazonaws.com
docker tag dqlabs-server:latest 655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-server:$1
docker push 655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-server:$1
rm -rf Dqlabs-Server-2.0
cd ../../
kubectl set image deployment/dqlabs-server dqlabs-server=655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-server:$1 -n airflow
python notifier.py


