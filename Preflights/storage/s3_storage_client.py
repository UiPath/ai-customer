import logging
import os

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class StorageClient():
    def __init__(self, credentials, error_handler):
        logger.info("Initializing storage client.")
        self.client = None
        self.error_handler = error_handler
        self._set_service_account_for_client(credentials)
        logger.info("Storage client initialized.")

    def _set_service_account_for_client(self, credentials):

        try:
            self.client = boto3.client('s3',
                                       endpoint_url=credentials['endpoint_url'],
                                       aws_access_key_id=credentials['access_key'],
                                       aws_secret_access_key=credentials['secret_key'],
                                       config=Config(connect_timeout=10, retries={'max_attempts': 1}),
                                       verify=False)

        except Exception as e:
            self.error_handler("Failed to initialize s3 client, error: {}".format(str(e)))

    def buckets_exists(self, buckets: list):
        status = True
        for bucket in buckets:
            status = status and self.bucket_exists(bucket)
        return status

    def bucket_exists(self, bucket_name: str):
        try:
            self.client.head_bucket(Bucket=bucket_name)
            logger.info("Bucket:{} exists.".format(bucket_name))
            return True
        except ClientError:
            self.error_handler("Bucket:{} either not accessible or not available".format(bucket_name))
        except Exception as e:
            error__format = "Failed to access bucket:{}. Error: {}".format(bucket_name, str(e))
            self.error_handler(error__format)
        return False

    def copy_local_file_to_bucket(self, bucket_name, src_path, file_name, dest_path):
        try:
            fq_file_path = os.path.join(src_path, file_name)
            self.client.upload_file(fq_file_path, bucket_name, dest_path + file_name)
            logger.debug("Local file:{} copied to bucket:{} under path:{}".format(fq_file_path, bucket_name, dest_path))
        except Exception as e:
            self.error_handler(
                "Failed to copy local file:{} to bucket:{}. Error: {}".format(fq_file_path, bucket_name, str(e)))

    def copy_item_within_bucket(self, bucket, src_path, src_file_name, dest_path, dest_file_name):
        try:
            fq_src_file_path = os.path.join(src_path, src_file_name)
            fq_dest_file_path = os.path.join(dest_path, dest_file_name)
            copy_source = {
                'Bucket': bucket,
                'Key': fq_src_file_path
            }
            self.client.copy(copy_source, bucket, fq_dest_file_path)
            logger.debug(
                "Successfully copied file:{} to path:{} in bucket:{}.".format(fq_src_file_path, dest_path,
                                                                              bucket))
            return True
        except Exception as ex:
            self.error_handler(
                "Failed to copy file:{} to path:{} in bucket:{}. Error: {}".format(fq_src_file_path, dest_path, bucket,
                                                                                   str(ex)))
        return False

    def copy_item_across_bucket(self, src_bucket, src_path, file_name, dest_bucket, dest_path):
        try:
            fq_src_file_path = os.path.join(src_path, file_name)
            fq_dest_file_path = os.path.join(dest_path, file_name)
            copy_source = {
                'Bucket': src_bucket,
                'Key': fq_src_file_path
            }
            self.client.copy(copy_source, dest_bucket, fq_dest_file_path)
            logger.debug(
                "Successfully copied file:{} from bucket:{} to path:{} in bucket:{}.".format(fq_src_file_path,
                                                                                             src_bucket,
                                                                                             dest_path,
                                                                                             dest_bucket))
            return True
        except Exception as ex:
            self.error_handler(
                "Failed to copy file:{} from bucket:{} to path:{} in bucket:{}. Error: {}".format(fq_src_file_path,
                                                                                                  src_bucket,
                                                                                                  fq_dest_file_path,
                                                                                                  dest_bucket,
                                                                                                  str(ex)))
        return False

    def list_blobs(self, bucket_name, path):
        try:
            done = False
            marker = None
            keys = []
            while not done:
                if marker:
                    data = self.client.list_objects(Bucket=bucket_name, MaxKeys=1000,
                                                    Marker=marker, Prefix=path)
                else:
                    data = self.client.list_objects(Bucket=bucket_name, MaxKeys=1000,
                                                    Prefix=path)
                if 'Contents' in data:
                    for key in data['Contents']:
                        keys.append(key['Key'])
                else:
                    return keys
                if data['IsTruncated']:
                    marker = data['Contents'][-1]['Key']
                else:
                    done = True
            return keys
        except Exception as e:
            self.error_handler("Failed to list file/directory for bucket {}.Error: {}.".format(bucket_name, str(e)))

    def upload_multipart(self, bucket_name, bucket_path, file_path, file_name):
        if file_path and file_name:
            try:
                config = TransferConfig(multipart_threshold=1024 * 5,
                                        max_concurrency=10,
                                        multipart_chunksize=1024 * 5,
                                        use_threads=True)
                fq_bucket_file_path = os.path.join(bucket_path, file_name)
                fq_file_path = os.path.join(file_path, file_name)
                self.client.upload_file(fq_file_path, bucket_name, fq_bucket_file_path, Config=config)
                logger.debug(
                    "Multipart upload successful for file:{} to path:{} in bucket:{}.".format(fq_file_path,
                                                                                              bucket_path,
                                                                                              bucket_name))
                return True
            except Exception as ex:
                self.error_handler(
                    "Multipart upload failed for file:{} to path:{} in bucket:{}.Error:{}".format(file_path,
                                                                                                  bucket_path,
                                                                                                  bucket_name, str(ex)))

        return False

    def download_multipart(self, bucket_name, bucket_path, bucket_file_name, file_path, file_name):
        try:
            config = TransferConfig(multipart_threshold=1024 * 5,
                                    max_concurrency=10,
                                    multipart_chunksize=1024 * 5,
                                    use_threads=True)
            fq_bucket_file_path = os.path.join(bucket_path, bucket_file_name)
            fq_file_path = os.path.join(file_path, file_name)

            self.client.download_file(bucket_name, fq_bucket_file_path, fq_file_path, Config=config)
            logger.debug(
                "Multipart download successful for file:{} to path:{} from bucket:{}.".format(bucket_file_name,
                                                                                              fq_file_path,
                                                                                              bucket_name))
            return fq_file_path
        except Exception as ex:
            self.error_handler(
                "Multipart download failed for file:{} to path:{} from bucket:{}.Error:{}".format(bucket_file_name,
                                                                                                  fq_file_path,
                                                                                                  bucket_name, str(ex)))

        return None

    def delete_object(self, bucket_name, file_name):
        try:
            self.client.delete_object(Bucket=bucket_name,
                                      Key=file_name)

            logger.debug("Object:{} successfully deleted from bucket:{}.".format(file_name,
                                                                                 bucket_name))
        except Exception as e:
            self.error_handler("Failed to delete object:{} from bucket:{}.Error:{}.".format(file_name,
                                                                                            bucket_name,
                                                                                            str(e)))
            raise e

    def delete_objects(self, bucket, objects):
        objects_to_delete = [{'Key': obj} for obj in objects]
        try:
            self.client.delete_objects(Bucket=bucket,
                                       Delete={'Objects': objects_to_delete})

            logger.debug("Objects:{} successfully deleted from bucket:{}.".format(objects,
                                                                                  bucket))
            return objects_to_delete
        except Exception as e:
            self.error_handler("Failed to delete objects:{} from bucket:{}. Error:{}".format(objects,
                                                                                             bucket,
                                                                                             str(e)))

        return None

    def is_file_exists(self, bucket_name, bucket_directory_name, filename):
        blobs = self.list_blobs(bucket_name, bucket_directory_name)
        for blob in blobs:
            blob_name = blob.replace(bucket_directory_name + "/", '')
            if filename in blob_name:
                return True
        return False

    def generate_presigned_upload_url(self, bucket, bucket_path, bucket_file_name):
        return self.generate_presigned_url(bucket, bucket_path, bucket_file_name,
                                           lambda fq_bucket_file_path: self.client.generate_presigned_post(bucket,
                                                                                                           fq_bucket_file_path),
                                           "upload")

    def generate_presigned_download_url(self, bucket, bucket_path, bucket_file_name):
        return self.generate_presigned_url(bucket, bucket_path, bucket_file_name,
                                           lambda fq_bucket_file_path: self.client.generate_presigned_url('get_object',
                                                                                                          {
                                                                                                              'Bucket': bucket,
                                                                                                              'Key': fq_bucket_file_path}),
                                           "download")

    def generate_presigned_url(self, bucket, bucket_path, bucket_file_name, presigned_func, operation_type):
        try:
            fq_bucket_file_path = os.path.join(bucket_path, bucket_file_name)
            presigned_url = presigned_func(fq_bucket_file_path)
            logger.debug(
                "Presigned {} url generated for file:{} in bucket:{}.".format(
                    operation_type,
                    fq_bucket_file_path,
                    bucket))
            return presigned_url
        except Exception as e:
            self.error_handler(
                "Failed to generate presigned {} url for file:{} in bucket:{}.Error:{}".format(
                    operation_type,
                    fq_bucket_file_path,
                    bucket,
                    str(e)))

        return None
