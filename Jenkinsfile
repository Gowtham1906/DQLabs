pipeline {
    agent any

    stages {
        stage('code checkout') {
            steps {
                // Get some code from a GitHub repository
              git credentialsId: 'sudhakar-token', url: 'git@github.com:DQLabs-Inc/DQLabs-Airflow.git', branch: 'main'
            }

        }
         stage('Image Build') {
            steps {
                dir("${env.WORKSPACE}/infra/airflow"){
                    sh '''
                    docker build -t dqlabs-airflow:latest .
                    '''

                }
            }
        }
         stage('Image push') {
            steps {
                    sh '''
                    aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 655275087384.dkr.ecr.us-east-1.amazonaws.com
                    docker tag dqlabs-airflow:latest 655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow:$BUILD_NUMBER
                    docker push 655275087384.dkr.ecr.us-east-1.amazonaws.com/dqlabs-airflow:$BUILD_NUMBER

                    '''
            }
        }
    }
}
