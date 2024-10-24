import logging
import time
from typing import Generator

from spaceone.core.error import ERROR_REQUIRED_PARAMETER
from spaceone.inventory.plugin.collector.lib.server import CollectorPluginServer
from .manager import AssetManager
from .manager import StorageManager

app = CollectorPluginServer()

_LOGGER = logging.getLogger("spaceone")


@app.route("Collector.init")
def collector_init(params: dict) -> dict:
    return _create_init_metadata()


@app.route("Collector.collect")
def collector_collect(params: dict) -> Generator[dict, None, None]:
    options = params["options"]
    secret_data = params["secret_data"]
    schema = params.get("schema")
    project_id = secret_data.get("project_id")

    _check_secret_data(secret_data)

    bucket_name = options.get("bucket_name")
    if not bucket_name:
        raise ERROR_REQUIRED_PARAMETER(key="options.bucket_name")

    start_time = time.time()
    _LOGGER.debug(
        f"[collector_collect] Start Collecting Cloud Resources (project_id: {project_id}, bucket_name: {bucket_name})"
    )

    assets_info = StorageManager().get_assets_info(options, secret_data)
    for asset_info in assets_info:
        yield from AssetManager(
            asset_info=asset_info, options=options, secret_data=secret_data
        ).collect_resources(options, secret_data, schema)

    _LOGGER.debug(
        f"[collector_collect] Finished Collecting Cloud Resources "
        f"(project_id: {project_id}, bucket_name: {bucket_name}, duration: {time.time() - start_time:.2f}s)"
    )


def _create_init_metadata() -> dict:
    return {
        "metadata": {
            "supported_resource_type": [
                "inventory.CloudService",
                "inventory.CloudServiceType",
                "inventory.Region",
                "inventory.ErrorResource",
            ],
            "options_schema": {
                "type": "object",
                "properties": {
                    "bucket_name": {
                        "type": "string",
                        "title": "Bucket Name",
                        "description": "The name of the Google Cloud Storage bucket",
                    }
                },
                "required": ["bucket_name"],
            },
        },
    }


def _check_secret_data(secret_data: dict) -> None:
    required_keys = [
        "type",
        "private_key_id",
        "private_key",
        "client_email",
        "client_id",
        "auth_uri",
        "token_uri",
        "auth_provider_x509_cert_url",
        "project_id",
    ]

    for key in required_keys:
        if key not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key=f"secret_data.{key}")
