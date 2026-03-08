import pandas as pd
from psycopg2.extras import Json
from itertools import chain
import requests
import time
from typing import Dict, List, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
import math
from dqlabs.app_helper.db_helper import fetchall, execute_query
from dqlabs.app_helper.dag_helper import execute_native_query
from dqlabs.app_helper.crypto_helper import decrypt
from collections import defaultdict
import json
import ast
from typing import List, Dict, Union
import yaml
from urllib.parse import urljoin
from dqlabs.app_helper.log_helper import log_error


class APIDataValidator:
    def __init__(self, config: Dict, lookup_metadata: dict, measure: dict):
        """
        Initializes the LookupAPI class.

        Args:
            config (Dict): The configuration dictionary.
            lookup_metadata (dict): The metadata for the lookup.
            measure (dict): The measure dictionary.

        Attributes:
            config (Dict): The configuration dictionary.
            df (None): The DataFrame object.
            valid_count (int): The count of valid records.
            invalid_count (int): The count of invalid records.
            skipped_records (int): The count of skipped records.
            updated_config (dict): The updated configuration dictionary.
            duplicate_map (defaultdict): The dictionary to store duplicate records.
            measure (dict): The measure dictionary.
            measure_id (str): The measure ID.
            failed_rows (list): The list of failed rows.
            failed_rows_limit (int): The limit for the number of failed rows.
            query_key (str): The query key.
            request_field (list): The list of request fields.
        """
        api_url = lookup_metadata.get("source", "")
        authorization = lookup_metadata.get("authorization", {})
        self.lookup_type = lookup_metadata.get("type", "")
        throttle_limit = lookup_metadata.get("throttle_limit", 10)
        lookup_properties = measure.get("properties", {})
        api_config = {
            "url": api_url,
            "throttle_limit": throttle_limit,
        }
        self.config = config
        self.df = None
        self.valid_count = 0
        self.invalid_count = 0
        self.skipped_records = 0
        self.updated_config = self.extract_lookup_attributes(lookup_properties)
        self.updated_config.update({"api_config": api_config})
        self.duplicate_map = defaultdict(list)
        self.measure = config.get("measure")
        self.measure_id = measure.get("id")
        self.failed_rows = []
        self.failed_rows_limit = (
            config.get("dag_info", {})
            .get("settings", {})
            .get("reporting", {})
            .get("count", {})
        )
        self.failed_rows_limit = (
            int(self.failed_rows_limit) if self.failed_rows_limit else 0
        )

        # Initialize defaults
        self.is_swagger = False
        self.swagger_headers = []
        self.api_headers = []

        if isinstance(authorization, dict):
            auth = authorization
            is_swagger = auth.get("is_swagger") in [True, "True"]

            if is_swagger:
                self.is_swagger = True

                # Extract and assign Swagger details
                swagger_info = auth.get("swagger", {})
                self.swagger_url = swagger_info.get("url", "")
                self.swagger_headers = [
                    {"key": h["key"], "value": decrypt(h["value"])}
                    for h in swagger_info.get("header", [])
                    if h.get("key") and h.get("value")
                ]

                # Extract and assign API details
                api_info = auth.get("api", {})
                self.endpoint = api_info.get("url", "")
                self.api_headers = [
                    {"key": h["key"], "value": decrypt(h["value"])}
                    for h in api_info.get("header", [])
                    if h.get("key") and h.get("value")
                ]

        if not self.is_swagger:
            # Fallback if not Swagger
            for auth in authorization:
                key = auth.get("key")
                value = auth.get("value")
                if key and value:
                    api_config[key] = decrypt(value)

            query_attributes = self.updated_config.get("query_attributes", {})

            if query_attributes:
                self.query_key, self.request_field = list(query_attributes.items())[0]
                self.request_field = [field.lower() for field in self.request_field]


    def extract_lookup_attributes(self, lookup_data):
        """
        Extracts query and response attributes from the given lookup data.

        Args:
            lookup_data (dict): The lookup data containing lookup properties.

        Returns:
            dict: A dictionary containing the query attributes and response attributes.

        Example:
            lookup_data = {
                "lookup_properties": [
                    {
                        "type": "Input",
                        "referance_column": "input_column",
                        "lookup": [
                            {"name": "attribute1"},
                            {"name": "attribute2"}
                        ]
                    },
                    {
                        "type": "Output",
                        "referance_column": "output_column",
                        "lookup": [
                            {"name": "attribute3"},
                            {"name": "attribute4"}
                        ]
                    }
                ]
            }

            extract_lookup_attributes(lookup_data) will return:
            {
                "query_attributes": {"input_column": ["attribute1", "attribute2"]},
                "response_attributes": {"output_column": ["attribute3", "attribute4"]}
            }
        """
        query_attributes = {}
        response_attributes = {}
        lookup_properties = lookup_data.get("lookup_properties", [])
        for item in lookup_properties:
            lookup_type = item.get("type")  # 'Input' or 'Output'
            referance_column = item.get("referance_column")
            lookups = item.get("lookup", [])

            # We assume one attribute per lookup entry (like your payload)
            if lookups and referance_column:
                attribute_name = [
                    lookup.get("name") for lookup in lookups if "name" in lookup
                ]

                if lookup_type.lower() == "input":
                    query_attributes[referance_column] = attribute_name
                elif lookup_type.lower() == "output":
                    response_attributes[referance_column] = attribute_name

        return {
            "query_attributes": query_attributes,
            "response_attributes": response_attributes,
        }

    def fetch_data(self) -> pd.DataFrame:
        """
        Fetches data from the specified table and returns it as a pandas DataFrame.

        Returns:
            pd.DataFrame: The fetched data as a pandas DataFrame.
        """
        request_attrs = list(
            chain.from_iterable(self.updated_config["query_attributes"].values())
        )
        response_attrs = list(
            chain.from_iterable(self.updated_config["response_attributes"].values())
        )

        query = f"""
            SELECT *
            FROM {self.config['table_name']}
        """
        records, _ = execute_native_query(self.config, query, is_list=True)

        df = pd.DataFrame(records).convert_dtypes()
        df["record_index"] = df.index + 1
        df["is_valid_api_record"] = None  # Initialize column (None = not yet processed)
        return df

    def prepare_batches(self) -> List[List[Dict]]:
        """
        Create batches respecting throttle limits and collect duplicates.

        Returns:
            List[List[Dict]]: A list of batches, where each batch is a list of dictionaries representing unique records.
        """
        total_records = len(self.df)
        valid_mask = pd.Series([True] * len(self.df), index=self.df.index)
        for field in self.request_field:
            valid_mask &= self.df[field].notna()
            valid_mask &= self.df[field].astype(str).str.strip() != ""
        self.df.loc[~valid_mask, "is_valid_api_record"] = False
        self.skipped_records = sum(~valid_mask)
        self.failed_rows.extend(self.df[~valid_mask].to_dict("records"))
        clean_df = self.df[valid_mask].copy()
        grouped = clean_df.groupby(self.request_field)
        unique_records = []
        for key, group in grouped:
            if (
                len(self.request_field) == 1
                and "smarty" in self.updated_config["api_config"]["url"].lower()
            ):
                key = (key,) if len(self.request_field) == 1 else key
            group_dicts = group.to_dict("records")
            self.duplicate_map[str(key)] = group_dicts
            unique_records.append(group_dicts[0])

        self.skipped_records = total_records - len(clean_df)
        batch_size = self.updated_config["api_config"]["throttle_limit"]
        return [
            unique_records[i : i + batch_size]
            for i in range(0, len(unique_records), batch_size)
        ]

    def prepare_payload(self, batch: List[Dict]) -> Union[List[str], List[Dict]]:
        """
        Prepares the payload for the API request based on the given batch of records.

        Args:
            batch (List[Dict]): The batch of records to be processed.

        Returns:
            Union[List[str], List[Dict]]: The prepared payload for the API request.

        Raises:
            None

        """
        base_url = self.updated_config["api_config"]["url"]
        request_fields = self.request_field

        if "smarty" in base_url:
            payload = []
            for record in batch:
                entry = {}
                for field in request_fields:
                    value = record.get(field, "")
                    entry[field] = str(value)
                payload.append(entry)
        else:
            payload = [
                ", ".join(str(record.get(field, "")) for field in request_fields)
                for record in batch
            ]

        return payload

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
    def call_api(self, batch: List[Dict]) -> List[Dict]:
        """Call configured API with a batch of records.

        Args:
            batch (List[Dict]): A list of dictionaries representing the batch of records.

        Returns:
            List[Dict]: A list of dictionaries representing the response from the API.

        Raises:
            Exception: If the API call fails.

        """
        try:
            base_url = self.updated_config["api_config"]["url"]
            payload = self.prepare_payload(batch)
            auth_params = {
                k: v
                for k, v in self.updated_config["api_config"].items()
                if k not in ["url", "throttle_limit"]
            }
            auth_query = "&".join(
                f"{key}={value}" for key, value in auth_params.items()
            )

            # Final API URL
            full_url = f"{base_url}?{auth_query}"
            response = requests.post(
                full_url,
                json=payload,
                timeout=30,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return []

    def get_nested_value(self, data: dict, key_path: str) -> str:
        """Get value from nested dictionary using double underscore path like 'location__lat'"""
        keys = key_path.split(".")
        if self.is_swagger:
            data = data.get("response", {})
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key, {})
            elif isinstance(data, list):
                data = data[0].get(key, {})
            else:
                return ""
        return str(data)

    def record_matches_api_response(
        self, record: Dict, api_data: Dict, is_smarty: bool
    ) -> bool:
        """
        Check if the record matches the API response based on the specified attributes.

        Args:
            record (Dict): The record to be checked.
            api_data (Dict): The API response data.
            is_smarty (bool): Flag indicating if the API is a Smarty API.

        Returns:
            bool: True if the record matches the API response, False otherwise.
        """
        for api_field, db_field_list in self.updated_config.get(
            "response_attributes", {}
        ).items():
            db_field = db_field_list[0]  # unpack the single field name
            db_value = str(record.get(db_field, "")).lower()

            db_field = db_field.lower()
            if not db_field:
                continue

            db_value = str(record.get(db_field, "")).lower()
            api_value = self.get_nested_value(api_data, api_field).lower()
            if db_value != api_value:
                return False  # As soon as a mismatch is found
        return True  # All fields matched

    def process_query_val(self, query_val):
        """
        Process the query value by evaluating it as a literal expression.

        Args:
            query_val (str): The query value to be processed.

        Returns:
            str: The processed query value.

        """
        try:
            tuple_value = ast.literal_eval(query_val)
            result = (
                ", ".join(str(item) for item in tuple_value)
                if isinstance(tuple_value, tuple)
                else query_val
            )
        except Exception as e:
            result = query_val  # fallback to original if parsing fails
        return result

    def compare_responses(
        self, db_records: List[Dict], api_responses: Dict
    ) -> Tuple[int, int]:
        """
        Compare the database records with the API responses and determine the number of valid and invalid records.

        Args:
            db_records (List[Dict]): The list of database records to compare.
            api_responses (Dict): The API responses to compare.

        Returns:
            Tuple[int, int]: A tuple containing the number of valid and invalid records.

        """
        valid = invalid = 0
        api_lookup = {}
        is_smarty = "smarty" in self.updated_config["api_config"]["url"].lower()
        response_data = api_responses if is_smarty else api_responses.get("results", [])
        for idx, response in enumerate(response_data):
            if is_smarty:
                input_index = response.get("input_index")
                if "status" in response:
                    continue
                original_record = db_records[idx]
                key_values = []
                for field in self.request_field:
                    value = original_record.get(field, "")
                    if isinstance(value, (int, float)):
                        key_values.append(value)  # recent change
                    else:
                        key_values.append(str(value))
                query_str = str(tuple(key_values))
                api_lookup[query_str] = response
            else:
                query_value = str(response.get("query", ""))
                api_data = response.get("response", {})
                if "error" in api_data:
                    continue
                if api_data.get("results"):
                    best_result = max(
                        api_data["results"], key=lambda x: x.get("accuracy", 0)
                    )
                    api_lookup[
                        query_value
                    ] = best_result  # key change taking query_value as parameter by considering best accuracy

        # Now compare all duplicates for each original query value
        for query_val, dup_records in self.duplicate_map.items():
            result = str(query_val) if is_smarty else self.process_query_val(query_val)
            api_data = api_lookup.get(result)
            if not api_data:
                for record in dup_records:
                    record_idx = record["record_index"]  # Use your index column
                    self.df.loc[
                        self.df["record_index"] == record_idx, "is_valid_api_record"
                    ] = False  # Mark as invalid
                    invalid += 1
                    # invalid += len(dup_records)
                continue

            for record in dup_records:
                record_idx = record["record_index"]
                if self.record_matches_api_response(record, api_data, is_smarty):
                    self.df.loc[
                        self.df["record_index"] == record_idx, "is_valid_api_record"
                    ] = True
                    valid += 1
                else:
                    self.df.loc[
                        self.df["record_index"] == record_idx, "is_valid_api_record"
                    ] = False
                    invalid += 1
                    self.failed_rows.append(record)

        return valid, invalid

    def prepare_failed_rows(self):
        """
        Prepare the failed rows for further processing.

        Returns:
            str: A JSON string representing the failed rows.
        """
        df_clean = self.df.copy()
        columns_to_drop = ["record_index"]
        # Get all invalid records
        invalid_records = (
            df_clean[df_clean["is_valid_api_record"] == False]
            .drop(columns_to_drop, axis=1)
            .to_dict("records")
        )
        invalid_count = len(invalid_records)

        # Get all valid records (will be used if needed)
        valid_records = (
            df_clean[df_clean["is_valid_api_record"] == True]
            .drop(columns_to_drop, axis=1)
            .to_dict("records")
        )

        # Determine how many records we need from each category
        if invalid_count >= self.failed_rows_limit:
            # Case 1: Enough invalid records to fill the limit
            self.failed_rows = invalid_records[: self.failed_rows_limit]
        else:
            # Case 2: Need to supplement with valid records
            remaining_slots = self.failed_rows_limit - invalid_count
            self.failed_rows = invalid_records + valid_records[:remaining_slots]
        clean_failed_rows = self.sanitize_for_json(self.failed_rows)
        failed_rows_json = json.dumps(clean_failed_rows)
        return failed_rows_json

    def sanitize_for_json(self, obj):
        """Recursively convert NA-like values to None for JSON compatibility."""
        if isinstance(obj, list):
            return [self.sanitize_for_json(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self.sanitize_for_json(value) for key, value in obj.items()}
        elif pd.isna(obj):  # catches pd.NA, np.nan, None, etc.
            return None
        return obj

    def fetch_swagger_spec(self) -> dict:
        """Fetch and parse Swagger/OpenAPI specification, supporting both JSON and YAML"""
        # headers = {}
        if self.swagger_headers:
            headers = {
                item["key"]: self.decrypt(item["value"])
                for item in self.swagger_headers
                if item.get("key") and item.get("value")
            }
        else:
            headers = {}
        # Add authentication if needed

        response = requests.get(self.swagger_url, headers=headers)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")

        if "yaml" in content_type or self.swagger_url.endswith((".yaml", ".yml")):
            try:
                return yaml.safe_load(response.text)
            except yaml.YAMLError as e:
                raise ValueError(f"Failed to parse YAML Swagger spec: {str(e)}")
        else:
            # Default to JSON
            try:
                return response.json()
            except ValueError as e:
                raise ValueError(f"Failed to parse JSON Swagger spec: {str(e)}")

    def extract_base_url(self, swagger_spec: dict) -> str:
        """Extract base URL from Swagger spec"""
        schemes = swagger_spec.get("schemes", ["https"])
        scheme = schemes[0]  # Default to first scheme

        host = swagger_spec.get("host", "")
        base_path = swagger_spec.get("basePath", "")

        if not host:
            # Try servers array for OpenAPI 3.0
            servers = swagger_spec.get("servers", [{}])
            if servers and "url" in servers[0]:
                return servers[0]["url"]
            raise ValueError("No host defined in Swagger spec")

        return f"{scheme}://{host}{base_path}"

    def resolve_endpoint_schema(
        self, swagger_spec: dict, endpoint_path: str, method: str
    ) -> dict:
        """Enhanced to extract all parameter types and response schema"""
        path_item = swagger_spec["paths"].get(endpoint_path)
        if not path_item:
            raise ValueError(f"Endpoint {endpoint_path} not found")

        operation = path_item.get(method.lower())
        if not operation:
            raise ValueError(f"Method {method} not supported")

        # Extract all parameter types
        parameters = {"path": [], "query": [], "body": None, "header": []}

        # Combine operation and path level parameters
        all_params = operation.get("parameters", []) + path_item.get("parameters", [])

        for param in all_params:
            param_in = param.get("in")
            if param_in == "body":
                parameters["body"] = self.resolve_references(
                    swagger_spec, param.get("schema", {})
                )
            elif param_in in parameters:
                parameters[param_in].append(param)

        return {
            "method": method.lower(),
            "path": endpoint_path,
            "parameters": parameters,
            "responses": operation.get("responses", {}),
        }

    def resolve_references(
        self, swagger_spec: dict, schema: dict, visited_refs=None
    ) -> dict:
        """Enhanced to handle both internal and external $refs without breaking existing behavior"""
        if visited_refs is None:
            visited_refs = set()

        if not isinstance(schema, dict):
            return schema

        if "$ref" in schema:
            ref = schema["$ref"]

            if ref.startswith(("http://", "https://")):
                try:
                    # Split URL from path (e.g., "http://x.yaml#/foo")
                    ref_url, ref_path = ref.split("#", 1) if "#" in ref else (ref, "")
                    ref_path = f"#{ref_path}" if ref_path else "#"

                    external_spec = self._fetch_external_schema(ref_url)

                    if ref_path == "#":
                        return external_spec
                    parts = ref_path[1:].split("/")
                    resolved = external_spec
                    for part in parts:
                        resolved = resolved.get(part, {})
                    return self.resolve_references(swagger_spec, resolved, visited_refs)

                except Exception:
                    return schema

            elif ref.startswith("#/"):
                parts = ref[2:].split("/")
                resolved = swagger_spec
                for part in parts:
                    resolved = resolved.get(part, {})
                return self.resolve_references(swagger_spec, resolved, visited_refs)

            return schema

        if "properties" in schema:
            for prop, prop_schema in schema["properties"].items():
                schema["properties"][prop] = self.resolve_references(
                    swagger_spec, prop_schema, visited_refs
                )

        if "items" in schema:
            schema["items"] = self.resolve_references(
                swagger_spec, schema["items"], visited_refs
            )

        if "allOf" in schema:
            schema["allOf"] = [
                self.resolve_references(swagger_spec, x) for x in schema["allOf"]
            ]

        if "anyOf" in schema:
            schema["anyOf"] = [
                self.resolve_references(swagger_spec, x) for x in schema["anyOf"]
            ]

        if "oneOf" in schema:
            schema["oneOf"] = [
                self.resolve_references(swagger_spec, x) for x in schema["oneOf"]
            ]

        return schema

    def _fetch_external_schema(self, url: str) -> dict:
        """Helper to fetch and cache external schemas"""
        if url not in getattr(self, "_schema_cache", {}):
            response = requests.get(url, headers=self.auth_headers, timeout=10)
            response.raise_for_status()
            self._schema_cache[url] = (
                yaml.safe_load(response.text)
                if url.endswith((".yaml", ".yml"))
                else response.json()
            )
        return self._schema_cache[url]

    def parse_endpoint(self, endpoint: str) -> tuple:
        """Parse endpoint string into method and path"""
        if not endpoint.startswith("["):
            raise ValueError("Endpoint must start with [METHOD]")

        # Extract method and path
        method_end = endpoint.find("]")
        if method_end == -1:
            raise ValueError("Invalid endpoint format - missing closing ]")

        method = endpoint[1:method_end].strip().upper()
        path = endpoint[method_end + 1 :].strip()

        if not path.startswith("/"):
            path = "/" + path

        return method, path

    def construct_api_url(self, base_url: str, endpoint: str) -> str:
        """Construct full API URL ensuring proper path joining"""
        base_url = base_url.rstrip("/") + "/"
        clean_endpoint = endpoint.lstrip("/")
        return urljoin(base_url, clean_endpoint)

    def update_api_config(self, url: str, method: str, schema: dict):
        """Update API configuration with resolved details"""
        self.updated_config["api_config"].update({"url": url, "method": method})
        self.updated_config["api_schema"] = schema

    def prepare_data_for_validation(self):
        """Mark records with invalid query attributes without notes"""
        # Initialize all records as invalid by default
        self.df["is_valid_api_record"] = False

        # Get attribute mappings
        lookup_config = self.extract_lookup_attributes(
            self.measure.get("properties", {})
        )
        self.updated_config.update(
            {
                "query_attributes": lookup_config["query_attributes"],
                "response_attributes": lookup_config["response_attributes"],
            }
        )

        # Start with all records invalid, only mark valid if they pass all checks
        valid_mask = pd.Series(True, index=self.df.index)

        # Check all query attributes
        for col, attrs in lookup_config["query_attributes"].items():
            for attr in attrs:
                # Update mask for valid records (non-null and non-empty)
                valid_mask &= self.df[attr].notna() & (
                    self.df[attr].astype(str).str.strip() != ""
                )

        # Apply final validation status
        self.df["is_valid_api_record"] = valid_mask
        self.skipped_records = (~valid_mask).sum()

    def prepare_swagger_batches(self) -> List[List[Dict]]:
        """Prepare batches of only valid records"""
        valid_records = self.df[self.df["is_valid_api_record"]].to_dict("records")
        batch_size = self.updated_config["api_config"]["throttle_limit"]
        return [
            valid_records[i : i + batch_size]
            for i in range(0, len(valid_records), batch_size)
        ]

    def format_url_with_path_params(self, record: Dict) -> str:
        """Handle both path parameters and query parameters with proper validation"""
        url = self.updated_config["api_config"]["url"]
        endpoint_spec = self.updated_config["api_schema"]

        # First handle path parameters if they exist
        if endpoint_spec["parameters"]["path"]:
            for param in endpoint_spec["parameters"]["path"]:
                param_name = param["name"]
                if param_name in self.updated_config["query_attributes"]:
                    df_column = self.updated_config["query_attributes"][param_name][0]
                    param_value = str(record[df_column])
                    if not param_value:
                        raise ValueError(
                            f"Missing value for path parameter: {param_name}"
                        )
                    url = url.replace(f"{{{param_name}}}", param_value)

        # Then handle query parameters if they exist
        if endpoint_spec["parameters"]["query"]:
            query_params = []
            for param in endpoint_spec["parameters"]["query"]:
                param_name = param["name"]
                if param_name in self.updated_config["query_attributes"]:
                    df_column = self.updated_config["query_attributes"][param_name][0]
                    param_value = str(record[df_column])
                    if param_value:  # Only include non-empty values for query params
                        query_params.append(f"{param_name}={param_value}")

            if query_params:
                separator = "&" if "?" in url else "?"
                url = f"{url}{separator}{'&'.join(query_params)}"

        return url

    def prepare_request_body(self, record: Dict) -> Dict:
        """Constructs request body according to Swagger schema"""
        endpoint_spec = self.updated_config["api_schema"]
        body_schema = endpoint_spec["parameters"]["body"]
        if not body_schema:
            return None

        body = {}
        properties = body_schema.get("properties", {})
        for prop_name, prop_schema in properties.items():
            if prop_name in self.updated_config["query_attributes"]:
                df_column = self.updated_config["query_attributes"][prop_name][0]
                body[prop_name] = record[df_column]

        return body

    def make_api_request(
        self, url: str = None, params: Dict = None, json: Dict = None
    ) -> requests.Response:
        """Updated to handle all request types"""
        config = self.updated_config["api_config"]
        headers = {
            item["key"]: item["value"]
            for item in self.api_headers
            if item.get("key") and item.get("value")
        }

        # Add additional headers from config
        for key in ["api_key", "authorization"]:
            if key in config:
                headers[key] = config[key]

        method = config["method"].lower()
        request_url = url if url else config["url"]

        if method == "get":
            return requests.get(request_url, headers=headers, params=params)
        elif method == "post":
            return requests.post(request_url, headers=headers, json=json)
        else:
            raise ValueError(f"Unsupported method: {method}")

    def prepare_query_params(self, record: Dict) -> Dict:
        """Updated to use Swagger-validated parameter names"""
        params = {}
        endpoint_spec = self.updated_config["api_schema"]

        # Only include parameters defined in Swagger
        for param in endpoint_spec["parameters"]["query"]:
            param_name = param["name"]
            if param_name in self.updated_config["query_attributes"]:
                df_column = self.updated_config["query_attributes"][param_name][0]
                params[param_name] = record[df_column]
        return params

    def call_batch_api(self, batch: List[Dict]) -> List[Dict]:
        """Processes exactly one prepared batch without re-chunking"""
        api_responses = []
        endpoint_spec = self.updated_config["api_schema"]
        method = endpoint_spec["method"].lower()

        try:
            if method == "get":
                # GET - process records individually
                for record in batch:
                    url = self.format_url_with_path_params(record)
                    response = self.make_api_request(url=url)
                    api_responses.append(self._format_response(record, response))

            elif method == "post":
                # POST - send entire batch as single request
                if self.updated_config["api_config"]["throttle_limit"] > 1:
                    # Batch POST
                    batch_payload = [self.prepare_request_body(r) for r in batch]
                    response = self.make_api_request(json=batch_payload)

                    if response:
                        # Map batch response to individual records
                        for record, item_resp in zip(batch, response.json()):
                            api_responses.append(
                                self._format_response(
                                    record, item_resp, response.status_code
                                )
                            )
                    else:
                        api_responses.extend(
                            self._format_error(record, "Empty response")
                            for record in batch
                        )
                else:
                    # Single-record POST (throttle_limit=1)
                    for record in batch:
                        body = self.prepare_request_body(record)
                        response = self.make_api_request(json=body)
                        api_responses.append(self._format_response(record, response))

        except Exception as e:
            api_responses.extend(self._format_error(record, str(e)) for record in batch)

        return api_responses

    def _format_response(self, record, response, status_code=None):
        """Format API response for easier processing"""
        return {
            "record": record,
            "response": response.json() if response else None,
            "status_code": status_code or (response.status_code if response else None),
        }

    def _format_error(self, record, error):
        return {"record": record, "error": error, "status_code": None}

    def validate_response(self, record: Dict, response: Dict) -> bool:
        """Validate API response using extracted attributes"""
        response_attrs = self.updated_config["response_attributes"]
        for df_column, resp_keys in response_attrs.items():
            for resp_key in resp_keys:
                # Get value from response (support nested keys)
                resp_value = self.get_nested_value(response, resp_key)

                # Compare with dataframe value
                if str(record.get(resp_key, "")).strip().lower() != str(resp_value).strip().lower():
                 # if str(record[df_column]) != str(resp_value):
                    return False
        return True

    def process_batch_responses(self, batch: List[Dict], api_responses: List[Dict]):
        """Process and validate API responses for a batch"""

        endpoint_spec = self.updated_config["api_schema"]
        is_batch_post = (
            endpoint_spec["method"].lower() == "post"
            and self.updated_config["api_config"]["throttle_limit"] > 1
        )
        for api_response in api_responses:
            record = api_response["record"]

            if api_response.get("error") or not api_response.get("response"):
                self.invalid_count += 1
                self.df.loc[
                    self.df["id"] == record["id"], "is_valid_api_record"
                ] = False
                continue

            if is_batch_post:
                is_valid = self._validate_batch_post_response(
                    record, api_response["response"]
                )
            else:
                # EXISTING validation logic for GET/single POST
                is_valid = self.validate_response(record, api_response["response"])

            self._update_validation_status(record["id"], is_valid)

    def _validate_batch_post_response(self, record, response_item):
        """Special validation ONLY for batch POST responses"""

        try:
            # 1. Mandatory ID matching
            response_id = str(
                response_item.get("id") or response_item.get("request_id")
            )
            record_id = str(record.get("id") or record.get("product_id"))
            if response_id != record_id:
                return False

            # 2. Check validation flag if exists
            if "valid" in response_item and not response_item["valid"]:
                return False

            # 3. Additional checks from response_attributes config
            for df_col, api_fields in self.updated_config[
                "response_attributes"
            ].items():
                for api_field in api_fields:
                    if str(record.get(df_col)) != str(response_item.get(api_field)):
                        return False

            return True

        except Exception as e:
            return False

    def _update_validation_status(self, record_id, is_valid):
        """Update validation status in DataFrame"""
        if is_valid:
            self.valid_count += 1
            self.df.loc[self.df["id"] == record_id, "is_valid_api_record"] = True
        else:
            self.invalid_count += 1
            self.df.loc[self.df["id"] == record_id, "is_valid_api_record"] = False

    def generate_validation_results(self) -> Dict:
        """Generate final validation results"""

        return {
            "count": self.valid_count,
            "failed_rows": self.prepare_failed_rows(),
            "lookup_type": self.lookup_type,
        }

    def handle_parameterless_get(self):
        """Special handling for GET endpoints without parameters"""
        try:
            response = self.make_api_request()
            if response and response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            return None

    def validate_parameterless_response(self, swagger_spec, api_response: Dict) -> set:
        """Validate records using Swagger-defined response structure"""
        endpoint_spec = self.updated_config["api_schema"]
        response_attrs = list(self.updated_config["response_attributes"].values())

        # Get the expected response schema from Swagger
        success_response = endpoint_spec["responses"].get("200", {})
        response_schema = self.resolve_references(
            swagger_spec, success_response.get("schema", {})
        )

        api_combinations = set()

        try:
            # Extract the actual data array from the response
            if isinstance(api_response, dict) and "response" in api_response:
                # Handle wrapped response format
                response_data = api_response["response"]
                if isinstance(response_data, dict) and "data" in response_data:
                    items = response_data["data"]
                else:
                    items = (
                        response_data
                        if isinstance(response_data, list)
                        else [response_data]
                    )
            else:
                # Handle direct array response
                items = (
                    api_response if isinstance(api_response, list) else [api_response]
                )


            for item in items:
                try:
                    values = []
                    for attr_group in response_attrs:
                        # Get the first attribute name from each group
                        attr_name = attr_group[0]
                        value = self.get_swagger_nested_value(item, attr_name)
                        values.append(str(value) if value is not None else "")

                    combination = tuple(values)
                    api_combinations.add(combination)

                except Exception as e:
                    continue

        except Exception as e:
            print(f"Error processing API response: {str(e)}")

        return api_combinations

    def get_swagger_nested_value(self, data: Dict, key_path: str):
        """Get value from nested dictionary using dot notation"""
        if not data or not key_path:
            return None

        keys = key_path.split(".")
        value = data

        for key in keys:
            if value is None:
                return None
            if isinstance(value, list):
                # Handle arrays by getting values from all elements
                return [
                    self.get_swagger_nested_value(
                        item, ".".join(keys[keys.index(key) + 1 :])
                    )
                    for item in value
                ]
            elif isinstance(value, dict):
                value = value.get(key)
            else:
                return None

        return value

    def process_parameterless_results(self, api_combinations: set):
        """Update validation status for all records"""
        response_attrs = list(self.updated_config["response_attributes"].values())

        # Get unique combinations from DataFrame
        unique_combos = self.df[[attr[0] for attr in response_attrs]].drop_duplicates()

        # Create validation map
        validation_map = {}
        for _, row in unique_combos.iterrows():
            combo = tuple(str(row[attr[0]]) for attr in response_attrs)
            validation_map[combo] = combo in api_combinations

        # Apply validation to all records
        for idx, record in self.df.iterrows():
            combo = tuple(str(record[attr[0]]) for attr in response_attrs)
            is_valid = validation_map.get(combo, False)

            self.df.at[idx, "is_valid_api_record"] = is_valid
            if is_valid:
                self.valid_count += 1
            else:
                self.invalid_count += 1

    def run_validation(self) -> Dict[str, int]:
        """Execute full validation workflow"""
        if self.is_swagger:
            try:
                # Fetch and parse Swagger/OpenAPI specification
                swagger_spec = self.fetch_swagger_spec()

                # Extract base URL components
                base_url = self.extract_base_url(swagger_spec)

                # Parse endpoint method and path
                method, clean_endpoint = self.parse_endpoint(self.endpoint)

                # Resolve endpoint schema
                endpoint_schema = self.resolve_endpoint_schema(
                    swagger_spec, clean_endpoint, method
                )

                # Update API config with Swagger-derived values
                full_api_url = self.construct_api_url(base_url, clean_endpoint)
                self.update_api_config(full_api_url, method.lower(), endpoint_schema)
                self.df = self.fetch_data()
                self.prepare_data_for_validation()

                endpoint_spec = self.updated_config["api_schema"]
                is_parameterless_get = (
                    endpoint_spec["method"] == "get"
                    and not endpoint_spec["parameters"]["path"]
                    and not endpoint_spec["parameters"]["query"]
                )

                if is_parameterless_get:
                    # Special handling for parameterless GET
                    api_response = self.handle_parameterless_get()
                    if api_response:
                        api_combinations = self.validate_parameterless_response(
                            swagger_spec, api_response
                        )
                        self.process_parameterless_results(api_combinations)
                    else:
                        self.df["is_valid_api_record"] = False
                        self.invalid_count = len(self.df)
                else:

                    batches = self.prepare_swagger_batches()
                    throttle_limit = self.updated_config["api_config"]["throttle_limit"]

                    for batch in batches:
                        api_responses = self.call_batch_api(batch)
                        self.process_batch_responses(batch, api_responses)
                        time.sleep(len(batch) / throttle_limit)

                return self.generate_validation_results()

            except Exception as e:
                log_error("Failed to process Swagger spec", str(e))
                return {
                    "count": 0,
                    "failed_rows": [],
                    "lookup_type": self.lookup_type,
                    "error": str(e),
                }
        else:
            self.df = self.fetch_data()
            batches = self.prepare_batches()

            throttle_limit = self.updated_config["api_config"]["throttle_limit"]

            for batch in batches:
                api_responses = self.call_api(batch)
                if not api_responses:
                    self.invalid_count += len(batch)
                    continue

                valid, invalid = self.compare_responses(batch, api_responses)
                self.valid_count += valid
                self.invalid_count += invalid
                time.sleep(len(batch) / throttle_limit)

            return {
                "count": self.valid_count,
                "failed_rows": self.prepare_failed_rows(),
                "lookup_type": self.lookup_type,
            }
