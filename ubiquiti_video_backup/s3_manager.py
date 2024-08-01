import boto3
from botocore.exceptions import ClientError
from fnmatch import fnmatch
import logging
from multiprocessing import Pool
import os

from rich.console import Console

logger = logging.getLogger('ui_backups')
#TODO: remove
# boto3.set_stream_logger('')


class S3Manager:
    s3_bucket_name = None
    bucket = None

    def __init__(self,):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.s3_bucket_name = os.getenv('S3_BUCKET_NAME',
                                        default='test-cli-bucket-dcf00912-1dc6-426d-b428-ff266975a5f9')
        
        self.bucket = self.s3_resource.Bucket(self.s3_bucket_name)
        
        logger.info(f'Configured self.')


    def _check_if_bucket_exists(self,) -> bool:
        buckets = self.s3_resource.buckets.all()
        logger.debug(f'Found {len(list(buckets))} buckets')
        for bucket in self.s3_resource.buckets.all():
            if bucket.name == self.s3_bucket_name:
                logger.debug(f'{self.s3_bucket_name} exists.')
                return True
        logger.debug(f'{self.s3_bucket_name} does NOT exist.')
        return False
        

    def _create_s3_bucket(self,) -> bool:
        if not self._check_if_bucket_exists():
            try:
                self.bucket = self.s3_resource.create_bucket(
                    Bucket = self.s3_bucket_name,
                )
            except ClientError as e:
                logger.error(f'received a client error trying to create the bucket: {e}')
                logger.error(f'bucket name: {self.s3_bucket_name}')
                raise e
            logger.debug(f'Created bucket with name {self.s3_bucket_name}')
        return True
    
    
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

    
    def _get_filename_with_date_time(self, file_with_path):
        """
            <path on host>/video/year/month/day/<video_name> is the format of the video
            files.
            Create a filename with the format <year>_<month>_<day>_<video_name>
        """
        logger.debug(f'extracting date/time of creation from filepath for file {file_with_path}')
        parts = file_with_path.split('/')
        idx = parts.index('video')
        filename = parts[idx + 1] + parts[idx + 2] + parts[idx + 3] + "_" + parts[idx + 4]
        logger.debug(f'new file name {filename}')
        return filename


    def _reduce_dict_of_files_for_upload(self, files_on_host_dict: dict = None) -> dict[str, str]:
            """
                Removes keys that exist in the target bucket from the dict of files on host
                returns the resulting dict.
            """
            logger.debug(f'before removing keys: {files_on_host_dict}')
            bucket_contents = self._get_bucket_contents()
            keys_in_s3 = [i.key for i in bucket_contents]
            for k in keys_in_s3: files_on_host_dict.pop(k, None)
            logger.debug(f'after removing keys: {files_on_host_dict}')


    def _upload_log(self, log_name='ubiquiti_scripts_backup.log'):
        try:
            response = self.bucket.upload_file(log_name, os.path.basename(log_name))
        except FileNotFoundError as e:
                failed_uploads.append(v)
        except ClientError as e:
            logger.error(f'received a client error trying to upload file {f} to bucket: {e}')
            raise e
        logger.debug(f'uploaded log file {log_name}')


    def upload_file_list(self, files: dict = None):
        """
            given a list of files, remove keys for files already in S3.
            Upload any remaining files to s3.
        """
        self._reduce_dict_of_files_for_upload(files)
        logger.debug(f'Deduplicated files. Next attempt to upload {len(files)}')
        failed_uploads = []
        console = Console()
        with console.status("[bold green] Uploading files...") as status:
            try:
                for k, v in files.items():
                    if "/var" in v:
                        # for some reason these fail
                        continue
                    fname = self._get_filename_with_date_time(v)
                    logger.debug(f'attempting to upload key = {fname}\t v = {v}')
                    response = self.bucket.upload_file(v, fname)
                    logger.debug(f'response for file {fname} {response}')
            except FileNotFoundError as e:
                failed_uploads.append(v)
            except ClientError as e:
                logger.error(f'received a client error trying to upload file {v} to bucket: {e}')
                raise e
        if len(failed_uploads):
            logger.error(f'Failed to upload {len(failed_uploads)} files to BUCKET {self.s3_bucket_name}')
        logger.info(f'Uploaded {len(files) - len(failed_uploads)} files to BUCKET {self.s3_bucket_name}')
        self._upload_log()
        return True