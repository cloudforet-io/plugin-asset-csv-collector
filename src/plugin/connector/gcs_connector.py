import logging
from google.cloud import storage
from plugin.connector import GoogleCloudConnector

__all__ = ["GCSConnector"]

_LOGGER = logging.getLogger("spaceone")


class GCSConnector(GoogleCloudConnector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = storage.Client(credentials=self.credentials)

    def get_bucket(self, bucket_name):
        return self.client.get_bucket(bucket_name)

    def list_blobs(self, bucket_name):
        return list(self.client.list_blobs(bucket_name))

    def get_blob(self, bucket_name, blob_name):
        return self.get_bucket(bucket_name).get_blob(blob_name)
