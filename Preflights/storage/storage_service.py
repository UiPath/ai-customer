import logging
import os
import traceback

import requests

from constants import *
from storage.s3_storage_client import StorageClient

logger = logging.getLogger(__name__)


class StorageService:

    def __init__(self, credentials, error_handler):
        self.storage_client = None
        self.local_files = []
        self.error_handler = error_handler
        self.credentials = credentials

    def __enter__(self):
        if not self.credentials:
            self.error_handler("Credentials cannot be null or empty", False)
            return None
        if not self.initialize():
            return None
        return self

    def initialize(self):
        logger.info("Initializing storage service.")
        try:
            self.storage_client = StorageClient(self.credentials, self.error_handler)
            if self.storage_client.buckets_exists(MANDATORY_BUCKETS) and not os.path.exists(TMP):
                os.mkdir(TMP)
                logger.debug("Temp folder:{} created.".format(TMP))
                self.local_files = self.create_local_files("file", "txt", 4)
                if self.local_files:
                    logger.info("Storage service initialized.")
                    return True
        except Exception as ex:
            self.error_handler("Failed to initialize.Error:{}".format(str(ex)), False)
        self.storage_client = None
        return False

    def validate_storage(self):
        self.local_file_operations()
        self.bucket_file_operations()
        self.presigned_url_operations()
        self.multipart_operations()

    def local_file_operations(self):
        bucket_files = self.copy_local_files_to_bucket(AIFABRIC_STAGING, self.local_files, BUCKET_ROOT_PATH)
        if self.validate_file_count(AIFABRIC_STAGING, bucket_files, BUCKET_ROOT_PATH):
            logger.info("Files copied.")
            logger.debug("{} files copied successfully.".format(len(bucket_files)))
        else:
            self.error_handler("Failed to copy local files to bucket.", False)
        truncated_bucket_files = self.delete_files_from_bucket(AIFABRIC_STAGING, bucket_files[2:])
        if self.validate_file_count(AIFABRIC_STAGING,
                                    [file for file in bucket_files if file not in truncated_bucket_files],
                                    BUCKET_ROOT_PATH):
            logger.info("Files deleted.")
            logger.debug("{} files deleted successfully.".format(len(bucket_files)))
        else:
            self.error_handler("Failed to delete files from bucket.", False)

    def create_local_files(self, file_prefix, file_extension, count):
        files = []
        for r in range(count):
            file_name = "{}_{}.{}".format(file_prefix, r, file_extension)
            files.append(file_name)
            if not self.create_file(TMP, file_name, "abc"):
                return []
        return files

    def is_configuration_valid(self, configuration):
        try:
            required_fields = configuration["accessKey"] and configuration["secretKey"] and (
                    configuration["externalHost"] or configuration["internalHost"]) and (
                                      configuration["externalPort"] or configuration["internalPort"])
            if not required_fields:
                self.error_handler(
                    "Required fields(accessKey,secretKey,externalHost/internalHost and externalPort/internalPort) can "
                    "only have a non-null or an non-empty value.", False)
                return False
            logger.info("Configurations validated.")
            return True
        except KeyError:
            self.error_handler(
                "One or many required fields(accessKey,secretKey,externalHost/internalHost and externalPort/internalPort) "
                "are "
                "missing.", False)

        return False

    def destroy(self):
        logger.info("Cleaning up initiated.")
        try:
            if self.local_files:
                if os.path.exists(TMP):
                    for f in os.listdir(TMP):
                        os.remove(os.path.join(TMP, f))
                    os.rmdir(TMP)
                    logger.debug("Temp folder:{} and it's contents deleted.".format(TMP))
                else:
                    self.error_handler("Temp folder:{} doesn't exist.".format(TMP), False)

            if self.storage_client:
                self.storage_client.delete_objects(AIFABRIC_STAGING,
                                                   self.storage_client.list_blobs(AIFABRIC_STAGING,
                                                                                  ""))
                logger.debug("Objects cleaned up from bucket:{}".format(AIFABRIC_STAGING))
                self.storage_client.delete_objects(TRAIN_DATA,
                                                   self.storage_client.list_blobs(TRAIN_DATA, ""))
                logger.debug("Objects cleaned up from bucket:{}".format(TRAIN_DATA))
        except Exception as ex:
            self.error_handler("Failed to cleanup.Error:{}".format(str(ex)))

    def create_file(self, path, file_name, content):
        # TODO check if path exists.
        fq_file_path = os.path.join(path, file_name)
        try:
            with open(fq_file_path, "w") as file:
                file.write(content)
            logger.debug("Successfully created file:{} under path:{}.".format(file_name, path))
            return fq_file_path
        except Exception as ex:
            self.error_handler("Failed to create file:{} under path:{}.Error:{}".format(file_name, path, str(ex)))

        return None

    def delete_files_from_bucket(self, bucket, files):
        logger.info("Deleting files from bucket.")
        logger.debug("Deleting {} files from bucket:{}.".format(files, bucket))
        if files and self.storage_client.delete_objects(bucket, files):
            return files
        return None

    def validate_file_count(self, bucket, files, path):
        return files and len(self.storage_client.list_blobs(bucket, path)) == len(files)

    def copy_local_files_to_bucket(self, bucket, files, path):
        logger.info("Copying local files to bucket.")
        logger.debug("Copying {} local files to bucket:{}.".format(files, bucket))
        bucket_files = []
        for file_name in files:
            bucket_files.append(path + file_name)
            if not self.create_file(TMP, file_name, "abc"):
                return []
            self.storage_client.copy_local_file_to_bucket(bucket, TMP, file_name, BUCKET_ROOT_PATH)
        return bucket_files

    def create_large_binary_object(self, path, file_name, size=BINARY_OBJ_SIZE):
        try:
            logger.info("Creating object for multi-part operations.")
            fq_file_path = os.path.join(path, file_name)
            with open(fq_file_path, 'wb') as fout:
                fout.write(os.urandom(size))
            logger.debug("Successfully created binary object:{} of size:{} in path:{}.".format(file_name, size, path))
            return file_name
        except Exception as ex:
            self.error_handler(
                "Failed to create binary object:{} of size:{} in path:{}.Error:{}".format(file_name, size, path,
                                                                                          str(ex)))
        return None

    def download_multipart(self, file_name):
        logger.info("Initiating multi-part download.")
        logger.debug(
            "Initiating multi-part download for file:{} in path of bucket:{}.".format(
                os.path.join(BUCKET_ROOT_PATH, file_name), AIFABRIC_STAGING))
        if self.storage_client.upload_multipart(AIFABRIC_STAGING, BUCKET_ROOT_PATH, TMP, file_name):
            fq_download_file_path = self.storage_client.download_multipart(AIFABRIC_STAGING, BUCKET_ROOT_PATH,
                                                                           BINARY_UPLOAD_OBJECT, TMP,
                                                                           BINARY_DOWNLOAD_OBJECT)
        try:
            if os.path.getsize(fq_download_file_path) == BINARY_OBJ_SIZE:
                logger.info("Multi-part download successful")
            else:
                self.error_handler("Multi-part downloaded file doesn't match the file uploaded.")
        except Exception as ex:
            self.error_handler("Multi-part download failed.Error:{}".format(str(ex)), False)

    def bucket_file_operations(self):
        file_name = self.local_files[0]
        self.copy_item_within_bucket(file_name)
        self.copy_item_across_bucket(file_name)

    def copy_item_across_bucket(self, file_name):
        logger.info("Copy file across buckets.")
        logger.debug(
            "Successfully copied file:{} from bucket:{} to path:{} in bucket:{}.".format(file_name,
                                                                                         AIFABRIC_STAGING,
                                                                                         BUCKET_ROOT_PATH,
                                                                                         TRAIN_DATA))
        if not self.storage_client.copy_item_across_bucket(AIFABRIC_STAGING, BUCKET_ROOT_PATH, file_name, TRAIN_DATA,
                                                           BUCKET_ROOT_PATH):
            self.error_handler("Failed to copy file across buckets.")
        else:
            logger.info("File copied across buckets.")

    def copy_item_within_bucket(self, file_name):
        logger.info("Copy file within a bucket.")
        logger.debug("Copying file:{} to path:{} in bucket:{}.".format(file_name, BUCKET_ROOT_PATH, AIFABRIC_STAGING))
        if not self.storage_client.copy_item_within_bucket(AIFABRIC_STAGING, BUCKET_ROOT_PATH, file_name,
                                                           BUCKET_SUB_PATH,
                                                           file_name):
            self.error_handler("Failed to copy file within a bucket.", False)
        else:
            logger.info("File copied within a bucket.")

    def multipart_operations(self):
        file_name = self.create_large_binary_object(TMP, BINARY_UPLOAD_OBJECT)
        if self.multipart_upload(file_name):
            self.download_multipart(file_name)

    def multipart_upload(self, file_name):

        logger.info("Initiating multi-part upload.")
        logger.debug(
            "Initiating multi-part upload for file:{} in path of bucket:{}.".format(
                os.path.join(TMP, file_name),
                BUCKET_ROOT_PATH, AIFABRIC_STAGING))
        if self.storage_client.upload_multipart(AIFABRIC_STAGING, BUCKET_ROOT_PATH, TMP, file_name):
            logger.info("Multi-part upload successful.")
            return True
        return False

    def presigned_url_operations(self):

        if self.presigned_url_upload_operations(AIFABRIC_STAGING, BUCKET_ROOT_PATH, PRESIGNED_UPLOAD_TXT):
            return self.presigned_url_download_operations(AIFABRIC_STAGING, BUCKET_ROOT_PATH, PRESIGNED_UPLOAD_TXT, TMP,
                                                          PRESIGNED_DOWNLOAD_TXT)
        return None

    def presigned_url_upload_operations(self, bucket, bucket_path, bucket_file_name):
        logger.info("Generating pre-signed upload url.")
        logger.debug(
            "Generating pre-signed upload url for file:{} in bucket:{}.".format(
                os.path.join(bucket_path, bucket_file_name),
                bucket))
        presigned_upload_url = self.storage_client.generate_presigned_upload_url(bucket, bucket_path,
                                                                                 bucket_file_name)
        if presigned_upload_url:
            try:
                file_name = self.local_files[0]
                fq_file_path = os.path.join(TMP, file_name)
                with open(fq_file_path, 'rb') as f:
                    files = {'file': (file_name, f)}
                    response = requests.post(presigned_upload_url['url'], data=presigned_upload_url['fields'],
                                             files=files, verify=False)
                    if response.status_code == 204:
                        logger.info("Pre-signed url upload successful.")
                        return True
                    self.error_handler(
                        "Pre-signed url upload failed.status_code:{},reason:{},text:{}.".
                            format(response.status_code,
                                   response.reason,
                                   response.text), False)
            except Exception as ex:
                self.error_handler(
                    "Pre-signed url upload operation failed for file:{} in path:{} for bucket:{}.Error:{}".
                        format(bucket_file_name,
                               bucket_path,
                               bucket,
                               str(ex)))

        return False

    def presigned_url_download_operations(self, bucket, bucket_path, bucket_file_name, local_file_path,
                                          local_file_name):
        try:
            logger.info("Generating pre-signed download url.")
            logger.debug(
                "Generating pre-signed download url for file:{} in bucket:{}.".format(
                    os.path.join(bucket_path, bucket_file_name),
                    bucket))
            presigned_download_url = self.storage_client.generate_presigned_download_url(bucket, bucket_path,
                                                                                         bucket_file_name)
            response = requests.get(presigned_download_url, allow_redirects=True, verify=False)
            if response.status_code == 200:
                fq_download_path = os.path.join(local_file_path, local_file_name)
                with open(fq_download_path, 'wb') as f:
                    f.write(response.content)
                logger.info("Pre-signed url download successful.")
            else:
                self.error_handler(
                    "Pre-signed url download failed.status_code:{},reason:{},text:{}.".
                        format(response.status_code,
                               response.reason,
                               response.text))
        except Exception as ex:
            self.error_handler("Pre-signed url download operation failed for file:{} in path:{} for bucket:{}.Error:{}".
                               format(bucket_file_name,
                                      bucket_path,
                                      bucket,
                                      str(ex)), False)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.destroy()
