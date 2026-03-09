import os
import boto3
from copy import deepcopy
from dqlabs.app_helper.dq_helper import get_client_domain
from io import BytesIO
import zipfile
import defusedxml.ElementTree as ET
import json

class S3StorageService:
    def __init__(self, storage_config: dict) -> None:
        config = deepcopy(storage_config) if storage_config else {}

        _access_key = config.get("access_key") if config.get("access_key") else os.environ.get("S3_ACCESS_KEY")
        _secret_key = config.get("secret_key") if config.get("secret_key") else os.environ.get("S3_SECRET_KEY")
        _region_name = config.get("region") if config.get("region") else os.environ.get("S3_REGION")
        self.bucket_name = config.get("bucket_name") if config.get("bucket_name") else os.environ.get("S3_BUCKET_NAME")

        self.base_url = (
            os.environ.get("S3_BASE_URL")
            .replace('<bucket_name>', self.bucket_name)
            .replace('<region_name>', _region_name)
        )
        self.client = boto3.client(
            "s3",
            aws_access_key_id=_access_key,
            aws_secret_access_key=_secret_key,
            region_name=_region_name
        )
        self.s3_resource = boto3.resource(
            's3',
            aws_access_key_id=_access_key,
            aws_secret_access_key=_secret_key,
            region_name=_region_name
        )

    def upload_file(self, file: object, file_path, dag_info: dict) -> str:
        """
        Uploads the given file object into s3 and
        returns the public file path
        """
        root_path = get_client_domain(dag_info)
        root_path = f"""{root_path}/{file_path}"""

        self.client.put_object(Key=root_path, Body=file,
                               Bucket=self.bucket_name)
        file_url = f"{self.base_url}{root_path}"
        return file_url

    def delete_file(self, file_path: str, dag_info: dict) -> str:
        """
        Delete the file from s3 bucket
        """
        root_path = get_client_domain(dag_info)
        root_path = f"""{root_path}/{file_path}"""

        response = self.client.delete_object(
            Key=root_path, Bucket=self.bucket_name)
        return response

    def download_file(self, file_path: str, dag_info: dict, local_path: str) -> str:
        """
        Download the file from s3 bucket
        """
        root_path = get_client_domain(dag_info)
        key_path = file_path.split(f"""{root_path}/""")
        file_dir = "/".join(local_path.split("/")[:-1])
        if file_dir and not os.path.exists(file_dir):
            os.makedirs(file_dir)
        if "references" in file_path:
            base_aws_url = self.base_url
            if not self.base_url.endswith("/"):
                base_aws_url = f"""{self.base_url}/"""
            path_details = file_path.split(base_aws_url)[-1]
            subpath = ""
            if path_details == file_path:
                path_details = path_details.split("/references")
                subpath = path_details[0].split("/")[-1:]
                subpath = "/".join(subpath)
                print(f"""{subpath}/references{path_details[1]}""")
                self.client.download_file(
                        self.bucket_name, f"""{subpath}/references{path_details[1]}""", local_path)
            else:
                path_details = path_details.split("/references")
                if len(path_details) == 2:
                    self.client.download_file(
                        self.bucket_name, f"""{path_details[0]}/references{path_details[1]}""", local_path)
        else:
            self.client.download_file(
                self.bucket_name, f"""{root_path}/{key_path[1]}""", local_path)

    def upload_streaming_file(self, dag_info: dict, response, destination_path: str, chunk_size = 8 * 1024 * 1024) -> str:
        """
        Upload a streaming file to an S3 bucket
        """
        root_path = get_client_domain(dag_info)
        destination_path = f"""{root_path}/{destination_path}"""
        # Create a file-like wrapper for streaming
        file_obj = BytesIO()
        # Stream the content in chunks
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:  # filter out keep-alive chunks
                file_obj.write(chunk)
        
        # Reset pointer to start of file
        file_obj.seek(0)
        
        # Upload to S3
        self.client.upload_fileobj(
            Fileobj=file_obj,
            Bucket=self.bucket_name,
            Key=destination_path
        )

    def stream_extract_on_s3(self, dag_info, source_key, target_prefix, chunk_size=1024 * 1024):
        """
        Stream and extract a zip file from S3 to another S3 location
        Args:
            source_key (str): S3 key of the zip file to extract
            target_prefix (str): S3 prefix where extracted files will be stored
            chunk_size (int): Size of chunks to read from the zip file
        """
        root_path = get_client_domain(dag_info)
        source_key = f"""{root_path}/{source_key}"""
        target_prefix = f"""{root_path}/{target_prefix}"""
        # Initialize streaming
        zip_obj = self.client.get_object(Bucket=self.bucket_name, Key=source_key)
        zip_stream = BytesIO()
        
        # Stream the file in chunks
        for chunk in zip_obj['Body'].iter_chunks(chunk_size=chunk_size):  # 1MB chunks
            zip_stream.write(chunk)
        
        zip_stream.seek(0)
        
        # Extract files
        with zipfile.ZipFile(zip_stream) as zip_ref:
            for file_info in zip_ref.infolist():
                if not file_info.is_dir():
                    with zip_ref.open(file_info) as file:
                        self.client.upload_fileobj(
                            file,
                            self.bucket_name,
                            f"{target_prefix}/{file_info.filename}"
                        )
    
    def stream_parse_xml_from_s3(self, dag_info, file_key):
        """
        Stream and parse large XML file from S3 and return as string
        Args:
            dag_info (dict): Context info (optional)
            file_key (str): Path to the XML file in S3
        Returns:
            str: Full XML content as string
        """
        try:
            # Stream the XML file from S3
            response = self.client.get_object(Bucket=self.bucket_name, Key=file_key)
            xml_file = response['Body']

            # Parse the XML fully
            tree = ET.parse(xml_file)
            root = tree.getroot()

            # Convert parsed XML back to string
            xml_string = ET.tostring(root, encoding='unicode')
            root = ET.fromstring(xml_string)

            return root

        except Exception as e:
            print(f"Error streaming/reading/parsing XML from S3: {e}")
            return None

    def stream_parse_json_from_s3(self, dag_info, file_key):
        try:
            # Get the file object with streaming
            response = self.client.get_object(Bucket=self.bucket_name, Key=file_key)
            file_content = ""
            json_content = {}
            file_content = response['Body'].read()
            file_stream = BytesIO(file_content)     # Read and parse the JSON content    
            json_content = json.load(file_stream)

            return json_content
        except Exception as e:
            print(f"Error getting object from S3: {e}")

    def list_files(self, dag_info, prefix: str) -> list:
        """
        List files in the S3 bucket with the given prefix
        """
        root_path = get_client_domain(dag_info)
        prefix = f"""{root_path}/{prefix}"""
        response = self.client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix
        )
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append(obj['Key'])
        return files

    def delete_s3_folder_versioned(self, dag_info, folder_prefix):
        """
        Delete all objects and versions under a prefix in versioned buckets
        
        Args:
            bucket_name (str): Name of the S3 bucket
            folder_prefix (str): Folder path (prefix) to delete
        """
        bucket = self.s3_resource.Bucket(self.bucket_name)
        root_path = get_client_domain(dag_info)
        folder_prefix = f"""{root_path}/{folder_prefix}"""
        if not folder_prefix.endswith('/'):
            folder_prefix += '/'
        
        try:
            # Delete all object versions
            bucket.object_versions.filter(Prefix=folder_prefix).delete()
            return True
        except Exception as e:
            print(f"Error deleting folder: {e}")
            return False