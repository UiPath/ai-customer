import logging
import os
import traceback

import urllib3

from constants import DEFAULT_EXTERNAL_PORT, DEFAULT_EXTERNAL_ACCESS_SCHEME, DEFAULT_INTERNAL_ACCESS_SCHEME
from storage.storage_service import StorageService

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # TODO


def main():
    external_host = os.getenv("OBJECT_STORAGE_EXTERNAL_HOST")
    access_key = os.getenv("OBJECT_STORAGE_ACCESSKEY")
    secret_key = os.getenv("OBJECT_STORAGE_SECRETKEY")
    if not (external_host and access_key and secret_key):
        logger.error("OBJECT_STORAGE_EXTERNAL_HOST or OBJECT_STORAGE_ACCESSKEY or OBJECT_STORAGE_SECRETKEY is not set.")
    else:
        validate_external_storage(external_host, access_key, secret_key)
        validate_internal_storage(access_key, secret_key)
    logger.info("Storage validation complete.")


def form_storage_credentials(access_scheme, host, port, access_key, secret_key):
    endpoint_url = "{}://{}:{}".format(access_scheme, host, port)
    logger.debug("endpoint_url:{}".format(endpoint_url))
    return {
        "endpoint_url": endpoint_url,
        "access_key": access_key,
        "secret_key": secret_key
    }


def validate_external_storage(external_host, access_key, secret_key):
    logger.info("Validating storage with external details.")
    external_port = os.getenv("OBJECT_STORAGE_EXTERNAL_PORT", DEFAULT_EXTERNAL_PORT)
    external_access_scheme = os.getenv("OBJECT_STORAGE_EXTERNAL_ACCESS_SCHEMA", DEFAULT_EXTERNAL_ACCESS_SCHEME)
    validate_storage(access_key, external_access_scheme, external_host, external_port, secret_key)


def validate_storage(access_key, access_scheme, host, port, secret_key):
    with StorageService(
            form_storage_credentials(
                access_scheme, host, port, access_key,
                secret_key), error_handler) as storage_service:
        if storage_service:
            storage_service.validate_storage()


def validate_internal_storage(access_key, secret_key):
    if os.getenv("INTERNAL", None):
        logger.info("Validating storage with internal details.")
        internal_port = os.getenv("OBJECT_STORAGE_PORT")
        internal_host = os.getenv("OBJECT_STORAGE_HOST")
        internal_access_scheme = os.getenv("OBJECT_STORAGE_ACCESS_SCHEMA", DEFAULT_INTERNAL_ACCESS_SCHEME)
        validate_storage(access_key, internal_access_scheme, internal_host, internal_port, secret_key)


global is_passed
is_passed = True


def error_handler(msg: str, trace_back=True):
    logger.error(msg)
    if trace_back:
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
