import logging
import re
from typing import Generator, List, Dict, Any
from collections import defaultdict
from spaceone.core.manager import BaseManager
from plugin.connector.gcs_connector import GCSConnector

_LOGGER = logging.getLogger("spaceone")

pattern = r"provider=([^/]+)/cloud_service_group=([^/]+)/cloud_service_type=([^/]+)/.+"


class StorageManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gcs_connector = None
        self.bucket_name = None

    def get_assets_info(self, options: dict, secret_data: dict) -> List[Dict[str, Any]]:
        self.gcs_connector = GCSConnector(options, secret_data)
        self.bucket_name = options.get("bucket_name")

        bucket = self.gcs_connector.get_bucket(self.bucket_name)
        blobs_path = [
            blob.name for blob in bucket.list_blobs() if re.match(pattern, blob.name)
        ]

        if not blobs_path:
            _LOGGER.debug(f"[get_assets_info] No assets found in {self.bucket_name}")

        return self._create_assets_info(blobs_path)

    def _create_assets_info(self, blobs_path: list) -> List[Dict[str, Any]]:
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
                groups[group_key]["csv_file_path"] = f"{self.bucket_name}/{blob_path}"
            elif file_name == "metadata.yaml" or file_name == "metadata.yml":
                groups[group_key][
                    "metadata_file_path"
                ] = f"{self.bucket_name}/{blob_path}"

        assets_info = [
            {
                "provider": provider,
                "cloud_service_group": cloud_service_group,
                "cloud_service_type": cloud_service_type,
                **files,
            }
            for (
                provider,
                cloud_service_group,
                cloud_service_type,
            ), files in groups.items()
        ]

        if not any("metadata_file_path" in files for files in groups.values()):
            if assets_info:
                assets_info[0]["is_primary"] = True

        _LOGGER.debug(f"[get_assets_info] assets_info: {assets_info}")

        return assets_info
