import abc
import logging
from typing import Generator

from spaceone.core.manager import BaseManager
from spaceone.core.error import ERROR_NOT_IMPLEMENTED
from spaceone.inventory.plugin.collector.lib import *

from plugin.conf.global_conf import REGION_INFO, ICON_URL_PREFIX

_LOGGER = logging.getLogger("spaceone")

__all__ = ["ResourceManager"]


class ResourceManager(BaseManager):
    service = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.provider = "google_cloud"
        self.cloud_service_group = None
        self.cloud_service_type = None
        self.service_code = None
        self.is_primary = False
        self.icon = None
        self.labels = []
        self.metadata_path = None

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def collect_resources(
        self, options: dict, secret_data: dict, schema: str
    ) -> Generator[dict, None, None]:
        try:

            _LOGGER.debug(
                f"[{self.__repr__()}] Collect cloud services: "
                f"{self.cloud_service_group} > {self.cloud_service_type}"
            )
            response_iterator = self.collect_cloud_services(
                options, secret_data, schema
            )
            for response in response_iterator:
                try:
                    yield make_response(
                        resource_type="inventory.CloudService",
                        cloud_service=response,
                        match_keys=[
                            [
                                "reference.resource_id",
                                "provider",
                                "cloud_service_type",
                                "cloud_service_group",
                            ]
                        ],
                    )
                except Exception as e:
                    _LOGGER.error(f"[{self.__repr__()}] Error: {str(e)}", exc_info=True)
                    yield make_error_response(
                        error=e,
                        provider=self.provider,
                        cloud_service_group=self.cloud_service_group,
                        cloud_service_type=self.cloud_service_type,
                    )

            _LOGGER.debug(
                f"[{self.__repr__()}] Collect cloud service type: "
                f"{self.cloud_service_group} > {self.cloud_service_type}"
            )
            yield self.get_cloud_service_type()

        except Exception as e:
            _LOGGER.error(f"[{self.__repr__()}] Error: {str(e)}", exc_info=True)
            yield make_error_response(
                error=e,
                provider=self.provider,
                cloud_service_group=self.cloud_service_group,
                cloud_service_type=self.cloud_service_type,
            )

    @abc.abstractmethod
    def collect_cloud_services(
        self, options: dict, secret_data: dict, schema: str
    ) -> Generator[dict, None, None]:
        raise ERROR_NOT_IMPLEMENTED()

    def get_cloud_service_type(self) -> dict:
        cloud_service_type = make_cloud_service_type(
            name=self.cloud_service_type,
            group=self.cloud_service_group,
            provider=self.provider,
            metadata_path=self.metadata_path,
            is_primary=self.is_primary,
            is_major=self.is_primary,
            service_code=self.service_code,
            tags={"spaceone:icon": f"{ICON_URL_PREFIX}/{self.icon}"},
            labels=self.labels,
        )

        return make_response(
            resource_type="inventory.CloudServiceType",
            cloud_service_type=cloud_service_type,
            match_keys=[["name", "group", "provider"]],
        )
