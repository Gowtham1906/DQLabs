git clone https://${GITHUB_TOKEN}@github.com/DQLabs-Inc/DQLabs-Airflow.git
cd DQLabs-Airflow
docker build -t dqlabs-airflow:latest
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 655275087384.dkr.ecr.us-east-1.amazonaws.com
docker tag dqlabs-airflow:latest 655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow:$1
docker push 655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow:$1