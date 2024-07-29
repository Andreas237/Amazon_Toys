import boto3
from botocore.exceptions import ClientError
from fnmatch import fnmatch
import logging
import os

logger = logging.getLogger('ui_backups')


class S3Manager:
    s3_bucket_name = None
    bucket = None

    def __init__(self,):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.s3_bucket_name = os.getenv('S3_BUCKET_NAME',
                                        default='udm_pro_video_backups_SloDown-5d1a2f64-718b-4f49-a45c-cce76e82d497')
        
        self.bucket = self.s3_resource.Bucket(self.s3_bucket_name)
        
        logger.info(f'Configured self.')


    def _create_s3_bucket(self,) -> bool:
        if not self._check_if_bucket_exists():
            try:
                self.bucket = self.s3_resource.create_bucket(
                    Bucket = self.s3_bucket_name,
                )
            except ClientError as e:
                logger.error(f'received a client error trying to create the bucket: {e}')
                raise e
            try:
                self.bucket.Versioning().enable()
            except ClientError as e:
                logger.error(f'received a client error trying to create the bucket: {e}')
                raise e
        logger.debug(f'Create bucket with name {self.s3_bucket_name}')
        return True


    def _check_if_bucket_exists(self,) -> bool:
        for bucket in self.s3_resource.buckets.all():
            logger.debug(f'bucket name: {bucket.name}')
            if bucket.name == self.s3_bucket_name:
                return True
        return False
        
    
    def _get_bucket_contents(self, prefix=None) -> list:
        contents = None
        try:
            if not self._create_s3_bucket():
                raise Exception(f"Could not create or find the requested bucket {self.bucket}\t{self.s3_bucket_name}")
            if not prefix:
                contents = list(self.bucket.objects.all())
            else:
                contents = list(self.bucket.objects.filter(Prefix=prefix))
        except ClientError as e:
            logger.error(f'received a client error trying to access bucket contents: {e}')
            raise e
        logger.debug(f'list of items in {self.s3_resource}:\n{contents}')
        return contents


    def _compare_bucket_contents_with_tracked_files(self, files_on_host: list = None) -> list:
        """
            Get the list of files matching the extensions. Return a list of files for upload
        """
        logger.debug(f'files on host: {files_on_host}')
        files_in_s3 = self._get_bucket_contents()
        logger.debug(f'files in s3: {files_in_s3}')
        # files_on_host = ['test_file.txt']
        files_for_upload = list(set(files_on_host) - set(files_in_s3) )
        logger.debug(f'files to upload: {files_for_upload}')
        return files_for_upload


    def upload_file_list(self, files: dict = None):
        logger.debug(f'attempting to upload file list {files}')
        files_for_upload = self._compare_bucket_contents_with_tracked_files(files.keys())
        try:
            for k,v in files_for_upload.items():
                object_name = os.path.basename(v)
                response = self.s3_client.upload_file(v, self.s3_bucket_name, object_name)
        except ClientError as e:
            logger.error(f'received a client error trying to upload file {f} to bucket: {e}')
            raise e
        logger.info(f'uploaded files {files}')
        return True