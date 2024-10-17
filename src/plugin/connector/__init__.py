import google.oauth2.service_account
import googleapiclient
import googleapiclient.discovery
import logging

from spaceone.core.connector import BaseConnector

_LOGGER = logging.getLogger(__name__)


class GoogleCloudConnector(BaseConnector):

    def __init__(self, options: dict, secret_data: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_id = secret_data["project_id"]
        self.credentials = (
            google.oauth2.service_account.Credentials.from_service_account_info(
                secret_data
            )
        )
