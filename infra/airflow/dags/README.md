# DQLabs-Airflow
This is a development environment setup document for `DQLabs Airflow Service` application in Linux. Please follow the below steps to setup your develpment environment.

**Table of Contents**

- [Development Environment Setup](#development-environment-setup)
- [Creating Virtual Environment](#creating-virtual-environment)
- [Apache Airflow - Configuration](#apache-airflow---configuration)
- [Setting Up Airflow Service](#setting-up-airflow-service)
- [Debugging Setup](#debugging-setup)
- [Deploying Airflow Module](#deploying-airflow-module)


## Development Environment Setup
The local development environment setup for dqlabs could be done in two ways:

- [Setting Up Local Environment](#setting-up-local-environment)
- [Setting Up Local Docker Environment (recommended)](#setting-up-local-docker-environment)


## Setting Up Local Environment

Before starting the development environment setup for `Airflow Service`, we need to setup the airflow in local environment.


### Creating Virtual Environment

In python application, we will be using a virtual environment for managing application specific modules and packages. The environment for each application can be created using `venv` or `conda`. Here is the reference to create virtual environments in your local environment.

- To create environment using `venv`, follow the below steps:
    - Create the environt using ``` python -m venv <path_to_env>/<environment_name> ```
    - After creating the environment, you can activate the environment using below steps:
        ```
            cd <path_to_env>/<environment_name>/Scripts

            activate
        ```

- To create environment using `conda`, follow the below steps:
    - Install the anaconda distribution from [`here`](#https://www.anaconda.com/products/distribution).
    - Create a virtual environment using conda by following the steps given [`here`](#https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-with-commands)


## Apache Airflow - Configuration

- Create a virtual environment for apache airflow using the steps given in [Creating Virtual Environment](#creating-virtual-environment) section .

- Activate the virtual environment.

- Create an airflow home dir and add it into `AIRFLOW_HOME` environment variable using below script.
    ```
        export AIRFLOW_HOME=~/airflow
    ```

- Install the airflow python package using
    ```
        pip install "apache-airflow==2.2.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.5/constraints-3.8.txt"
    ```
- Configure the postgresql db for airflow jobs using below script. [Setting up a postgresql database](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html#setting-up-a-postgresql-database).


    ```
        CREATE DATABASE airflow_db;

        <!-- You can set your own password for the airflow user -->
        CREATE USER airflow_user WITH PASSWORD '********';

        GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;

        <!-- The default search_path for new Postgres user is: "$user", public, no change is needed.  -->
        <!-- If you use a current Postgres user with custom search_path, search_path can be changed by the command:  -->
        ALTER USER airflow_user SET search_path = public;
    ```

- Configure the postgres database credential into `airflow.cfg` file using below script.

    ```
        vi ~/airflow/airflow.cfg

        <!-- Look for the  sql_alchemy_conn config vaiable and add the postgresql connection uri-->
        sql_alchemy_conn = postgresql+psycopg2://<user_name>:<password>@<host_name>/<db_name>

    ```

- Initialize the airflow db using `airflow db init`

- start the airflow webserver and scheduler using below script.

    ```
        <!-- To start the webserver UI -->
        airflow webserver

        <!-- To start the airflow scheduler -->
        airflow scheduler
    ```


## Setting Up Airflow Service

- Create a virtual environment for django server using the steps given in [Creating Virtual Environment](#creating-virtual-environment) section.

- Clone the [DQLabs Airflow Repo](https://github.com/DQLabs-Inc/DQLabs-Airflow.git) into your local environment.

- Install the dependent packagaes using below script.
    ```
        pip install -r requirements_dev.txt
    ```

**Note**: For the production we will use `requirements.txt` which has only the production dependency modules.

- Add the following env variables into the `dev.env` file.

```
    DEBUG=True
    LOG_BACKUP_COUNT=10
    SECRET_KEY=<add_django_auth_secret key>
    POSTGRESQL_DB_NAME=<db_name>
    POSTGRESQL_USER_NAME=<user_name>
    POSTGRESQL_PASSWORD=<password>
    POSTGRESQL_HOST=<host_name>
    POSTGRESQL_PORT=<port_number>
    AIRFLOW_HOME=<path_to_airflow_home_dir>
```

- After installing the dependencies execute the below command to initialize database.

    ```
        ENV=dev python manage.py init_server
    ```

**Note**: Set the `ENV=dev` environment variable for local environment before using the django commands. Also if you want to run the server from root dir of the repo, use `src/manage.py` instead of `manage.py` in the above script.

- Install the pre-commit packages to enable pre-commit validations:

```
    pre-commit install
```

- Start the server using `python manage.py runserver 0.0.0.0:8001` script, ther server will be started and available in `http://localhost:8001/`.

**Note**: When creating a new django app, use the command `make create_app name=<app_name>` to create a new app in django.


## Debugging Setup

Are you using VSCode? Please follow the steps below to enable the debugging mode server code.

- Configure the python interpreter to your workspace using following steps:
    - Open the command pallet and search for `Python: Select Interpreter` which will show you the list of available virtual environments on your local environment.

    - Select python interpreter which is available in your django server virtual environment that was created in [Setting Up Django Server](#setting-up-django-server) step.

- Configure the debugger by adding a `launch.json` file with following configurations in the .vscode dir:

    ```
        {
            "version": "0.2.0",
            "configurations": [
                {
                    "name": "Python: Django",
                    "type": "python",
                    "request": "launch",
                    "program": "${workspaceFolder}/src/manage.py",
                    "args": [
                        "runserver",
                        "0.0.0.0:8001"
                    ],
                    "django": true,
                    "justMyCode": true,
                    "env": {
                        "ENV": "dev"
                    }
                }
            ]
        }

    ```

Now you are able to run django application from vscode directly in debug mode. [Here](#https://code.visualstudio.com/docs/python/tutorial-django#_create-a-debugger-launch-profile) is the link to enable debugging mode in vscode.


## Setting Up Local Docker Environment
To setup the local development using docker, use the below steps:

- Install docker in your local development env from [here](#https://docs.docker.com/engine/install/). Here is the version details for docker setup:

```
    docker 20.10.17+
    docker-compose 1.28.2+
```

- Please make sure that you have installed both docker and docker-compose with the latest stable version.

- Clone the airflow repo from [here](#https://github.com/DQLabs-Inc/DQLabs-Airflow).

- Open the terminal and go to `infra` dir in airflow repo.

- Execute `sudo sh start-airflow.sh <build_number>` command which will up all the required containers in docker. The `<build_number>` can be any decimal version number. Here is the example to start the docker container `sudo sh start-airflow.sh 1.0`.

- Once all the containers are up and healthy, check whether airflow is running or not in your browser using airflow url `http://localhost:8080/`. (It will take some time to render the airflow webserver UI in browser).


## Deploying Airflow Module
Here is the steps to mannualy deploy the dqlabs airflow module into mwaa instance

- Clone the airflow repo from [here](#https://github.com/DQLabs-Inc/DQLabs-Airflow) and install the required modules in to a virtual env specific to this repo.

- Copy the env variables from  environments/<mwaa/qa/uat/prod>.env file of which environment we are going to be deploy and paste it into .env file. For example if you are deploying to the QA instance, then copy the env variables from qa.env file under environments dir into .env file.

- Open the terminal and go to `infra/airflow/dags/` dir in airflow repo.

- Execute the following commands to get the updated dqlabs module

```
    rm -rf dist/ build/

    python setup.py bdist_wheel

    cd dist/

    chmod -R 755 ./dqlabs-2.0-py3-none-any.whl

    zip -r plugins.zip ./dqlabs-2.0-py3-none-any.whl
```

- Once the plugins.zip file is generated, then move the `plugins.zip` into corresponding s3 bucket and update the mwaa instance with the latest plugins.zip file.

**Note :**
For MWAA instance, use the file `/infra/airflow/mwaa_requirements.txt` as requirements.txt file.
