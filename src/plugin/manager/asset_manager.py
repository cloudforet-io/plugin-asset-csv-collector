import logging
import yaml
import io
import pandas as pd
from typing import Generator
from spaceone.inventory.plugin.collector.lib import *
from spaceone.core.error import *
from plugin.manager.base import ResourceManager
from plugin.connector.gcs_connector import GCSConnector

_LOGGER = logging.getLogger("spaceone")

REQUIRED_COLUMNS = ["name"]


class AssetManager(ResourceManager):
    service = "Asset"

    def __init__(self, asset_info, options, secret_data, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gcs_connector = GCSConnector(options, secret_data)
        self.provider = asset_info["provider"]
        self.cloud_service_group = asset_info["cloud_service_group"]
        self.cloud_service_type = asset_info["cloud_service_type"]
        self.csv_file_path = asset_info["csv_file_path"]

        self.metadata = {}

        if metadata_file_path := asset_info.get("metadata_file_path"):
            self._initialize_metadata(metadata_file_path)

    def collect_cloud_services(
        self, options: dict, secret_data: dict, schema: str
    ) -> Generator[dict, None, None]:

        bucket_name, csv_file_path = self.csv_file_path.split("/", 1)
        bucket = self.gcs_connector.get_bucket(bucket_name)
        blob = bucket.get_blob(csv_file_path)
        text = blob.download_as_text()
        data_frame = pd.read_csv(io.StringIO(text))

        columns = list(data_frame.columns)
        self._check_data_columns(columns)

        if not self.metadata:
            self._create_default_metadata(columns)

        for index, row in data_frame.iterrows():
            yield self.make_cloud_service(row)

    def make_cloud_service(self, row: pd.Series) -> dict:
        name = row["name"]
        account = row.get("account")
        region_code = row.get("region_code")

        default_resource_id = f"{self.provider}:{self.cloud_service_group}:{self.cloud_service_type}:{name}"
        if unique_id := row.get("unique_id"):
            default_resource_id = unique_id

        resource_id = row.get("resource_id", default_resource_id)

        columns = list(row.index)

        data = {}
        for column in columns:
            if column.startswith("data."):
                data[column.split("data.")[1]] = row[column]

        return make_cloud_service(
            name=name,
            cloud_service_type=self.cloud_service_type,
            cloud_service_group=self.cloud_service_group,
            provider=self.provider,
            account=account,
            data=data,
            region_code=region_code,
            reference={
                "resource_id": resource_id,
            },
        )

    def get_cloud_service_type(self) -> dict:
        cloud_service_type = make_cloud_service_type(
            name=self.cloud_service_type,
            group=self.cloud_service_group,
            provider=self.provider,
            metadata=self.metadata,
            is_primary=self.is_primary,
            is_major=self.is_primary,
            service_code=self.service_code,
            tags={"spaceone:icon": self.icon},
            labels=self.labels,
        )

        return make_response(
            resource_type="inventory.CloudServiceType",
            cloud_service_type=cloud_service_type,
            match_keys=[["name", "group", "provider"]],
        )

    def _initialize_metadata(self, metadata_file_path):
        bucket_name, metadata_file_path = metadata_file_path.split("/", 1)
        bucket = self.gcs_connector.get_bucket(bucket_name)
        blob = bucket.get_blob(metadata_file_path)

        if blob:
            metadata = blob.download_as_text()
            metadata_dict = self.yaml_to_dict(metadata)

            if "icon" in metadata_dict:
                self.icon = metadata_dict["icon"]
                del metadata_dict["icon"]

            if "is_primary" in metadata_dict:
                self.is_primary = metadata_dict["is_primary"]
                del metadata_dict["is_primary"]

            if "search" in metadata_dict:
                self.metadata["search"] = metadata_dict["search"]

            if "table" in metadata_dict:
                self.metadata["table"] = metadata_dict["table"]

    @staticmethod
    def yaml_to_dict(yaml_str):
        try:
            return yaml.safe_load(yaml_str)
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML: {exc}")
            return

    @staticmethod
    def _check_data_columns(columns):
        for column in REQUIRED_COLUMNS:
            if column not in columns:
                raise ERROR_REQUIRED_PARAMETER(key=column)

    def _create_default_metadata(self, columns):

        self.metadata["search"] = {"fields": []}
        self.metadata["table"] = {"sort": {"key": "name"}, "fields": []}

        for column in columns:
            if column not in REQUIRED_COLUMNS and column.startswith("data."):
                column_name = column.split("data.")[1]
                self.metadata["search"]["fields"].append({column_name: column})
                self.metadata["table"]["fields"].append({column_name: column})