import json
import os
import pandas as pd
from io import BufferedReader
import requests
from dqlabs.app_helper.dq_helper import get_client_domain


class LocalStorageService:
    def __init__(self, storage_config: dict) -> None:
        self.config = storage_config


    def upload_file(self, file: object, file_path, dag_info: dict) -> str:
        """
        Uploads the given file object into disc and
        returns the file path
        """
        domain = get_client_domain(dag_info)
        server_endpoint = os.environ.get("DQLABS_SERVER_ENDPOINT")
        if server_endpoint and server_endpoint.endswith("/"):
            server_endpoint = "/".join(server_endpoint.split("/")[:-1])

        endpoint = f"{server_endpoint}/api/storage/"
        api_headers = {}
        data = {
            "organization_id": dag_info.get("organization_id"),
            "file_path": file_path,
            "domain": domain
        }

        temp_file_path = None
        if file and not isinstance(file, BufferedReader):
            temp_file_name = file_path.split('/')[-1]
            temp_file_location = os.environ.get("LOCAL_DIR")
            if temp_file_location and not os.path.exists(temp_file_location):
                os.makedirs(temp_file_location)

            temp_file_path = f"{temp_file_location}/{temp_file_name}"
            with open(temp_file_path, "wb") as local_file:
                local_file.write(file)
            file = open(temp_file_path, "rb")

        input_data = {
            "file_to_upload": file,
        }
        response = requests.post(url=endpoint, headers=api_headers, files=input_data, data=data)
        if temp_file_path:
            os.remove(temp_file_path)
            
        if response.status_code not in [200, 201]:
            raise Exception(response)
        api_response = response.json()
        api_response = api_response if api_response else {}
        file_url = api_response.get("file_url")
        return file_url
    

    def delete_file(self, file_path: str, dag_info: dict) -> str:
        """
        Delete the file from server disc
        """
        domain = get_client_domain(dag_info)
        server_endpoint = os.environ.get("DQLABS_SERVER_ENDPOINT")
        if server_endpoint and server_endpoint.endswith("/"):
            server_endpoint = "/".join(server_endpoint.split("/")[:-1])

        endpoint = f"{server_endpoint}/api/storage/delete/"
        api_headers = {
            'Content-Type': 'application/json',
        }
        
        data = {
            "organization_id": dag_info.get("organization_id"),
            "file_path": file_path,
            "domain": domain
        }
        data = json.dumps(data, default=str)
        response = requests.put(url=endpoint, headers=api_headers, data=data)
        return (response.status_code == 200)
    

    def download_file(self, file_path: str, dag_info: dict, local_path: str) -> str:
        """
        Download the file from server into airflow worker node
        """
        root_path = get_client_domain(dag_info)
        server_endpoint = os.environ.get("DQLABS_SERVER_ENDPOINT")
        # server_endpoint = "http://host.docker.internal:8000" ### Use this to connect local server
        if server_endpoint and server_endpoint.endswith("/"):
            server_endpoint = "/".join(server_endpoint.split("/")[:-1])
            
        endpoint = f"{server_endpoint}/api/storage/download/"
        api_headers = {
            'Content-Type': 'application/json',
        }
        
        data = {
            "organization_id": dag_info.get("organization_id"),
            "file_path": file_path,
            "domain": root_path
        }
        data = json.dumps(data, default=str)
        response = requests.put(url=endpoint, headers=api_headers, data=data)
        if response.status_code == 200:
            print("Downloaded the file successfully!")
            file = response.content
            if os.name.lower() == "nt":
                local_path = local_path.replace('\\', '/')
            local_path = local_path.replace('//', '/')

            file_dir = "/".join(local_path.split("/")[:-1])
            if file_dir and not os.path.exists(file_dir):
                os.makedirs(file_dir)
            
            file_extension = local_path.split(".")[-1]
            if file_extension.lower() == "xlsx" or file_extension.lower() == "xls":
                df = pd.read_excel(file,index_col=False, header=0)
                df.to_excel(local_path, index=False)
            else:
                with open(local_path, "wb") as destfile:
                    destfile.write(file)
        else:
            print("Downloading the file failed with error", response.text)
        
