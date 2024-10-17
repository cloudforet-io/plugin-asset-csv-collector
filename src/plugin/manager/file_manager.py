import logging
import re
from typing import Generator, List, Dict, Any
from collections import defaultdict
from spaceone.core.manager import BaseManager
from plugin.connector.gcs_connector import GCSConnector

_LOGGER = logging.getLogger("spaceone")

pattern = r"provider=([^/]+)/cloud_service_group=([^/]+)/cloud_service_type=([^/]+)/.+"


class FileManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gcs_connector = None

    def get_assets_info(self, options: dict, secret_data: dict) -> List[Dict[str, Any]]:
        self.gcs_connector = GCSConnector(options, secret_data)
        bucket_name = options.get("bucket_name")

        bucket = self.gcs_connector.get_bucket(bucket_name)
        blobs_path = [
            blob.name for blob in bucket.list_blobs() if re.match(pattern, blob.name)
        ]

        return self._create_assets_info(blobs_path, bucket_name)

    @staticmethod
    def _create_assets_info(blobs_path: list, bucket_name: str) -> List[Dict[str, Any]]:
        groups = defaultdict(dict)

        for blob_path in blobs_path:
            (
                provider_info,
                cloud_service_group_info,
                cloud_service_type_info,
                file_name,
            ) = blob_path.split("/")

            _, provider = provider_info.split("=")
            _, cloud_service_group = cloud_service_group_info.split("=")
            _, cloud_service_type = cloud_service_type_info.split("=")

            group_key = (provider, cloud_service_group, cloud_service_type)

            if file_name.endswith(".csv"):
                groups[group_key]["csv_file_path"] = f"{bucket_name}/{blob_path}"
            elif file_name == "metadata.yaml":
                groups[group_key]["metadata_file_path"] = f"{bucket_name}/{blob_path}"

        assets_info = []
        for (
            provider,
            cloud_service_group,
            cloud_service_type,
        ), files in groups.items():
            entry = {
                "provider": provider,
                "cloud_service_group": cloud_service_group,
                "cloud_service_type": cloud_service_type,
            }
            entry.update(files)
            assets_info.append(entry)

        return assets_info
