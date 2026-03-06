This is a Devops environment setup document for `DQLabs Docker Installation` application in Linux. Please follow the below steps
to setup your airflow docker environment.

### Docker Installation

Open Ubuntu terminal then use the command

```
    sudo apt-get remove docker docker-engine docker.io
    sudo apt-get update
    sudo apt install docker.io -y
    sudo snap install docker
```

- Once installing the docker, you can check the installation by verifying the docker version using `docker –-version`

- Install the airflow python package for docker using below scripts

    ```
       sudo usermod -a -G docker "$USER"
       sudo systemctl enable docker
       sudo curl -L "https://github.com/docker/compose/releases/download/1.28.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
       sudo chmod +x /usr/local/bin/docker-compose
       sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
       mkdir -p ./dags ./logs ./plugins
       echo -e "AIRFLOW_UID=$(id -u)" > .env
   ```

### Setting Up Service

- Clone the [DQLabs-Airflow Repo](#https://github.com/DQLabs-Inc/DQLabs-Airflow.git)

- Move to `infra` dir where the `docker-compose.yaml` file is located.

- Use the command `chmod 777 <file_name>`. In this example it is as follows, `chmod 777 docker-compose.yaml`


### Run the Docker compose file

- Use this command
        ```
            sudo docker-compose up airflow-init
        ```

### Need to Up the docker containers

- Enter this command

    ```
        sudo docker-compose up -d
    ```
### Run The Docker scheduler For manualy

- Follow this command
    ```
        docker exec -it --user root **container ID** bash
    ```

- Docker Installation setup completed

### Docker compose down and up command
     ```
        sudo docker-compose up -d
        sudo docker-compose down
    ```

### Docker Restart command

    ```
        docker restart  **container ID**
    ```
