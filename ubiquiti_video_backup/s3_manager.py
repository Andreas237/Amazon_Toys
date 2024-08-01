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


    def _check_if_bucket_exists(self,) -> bool:
        buckets = self.s3_resource.buckets.all()
        logger.debug(f'Found {len(list(buckets))} buckets')
        for bucket in self.s3_resource.buckets.all():
            if bucket.name == self.s3_bucket_name:
                logger.debug(f'{self.s3_bucket_name} exists.')
                return True
        logger.debug(f'{self.s3_bucket_name} does NOT exist.')
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
        files_on_host = list(files_on_host)
        bucket_contents = self._get_bucket_contents()
        files_in_s3 = [i.key for i in bucket_contents]
        logger.debug(f'files in s3: {files_in_s3}')
        files_for_upload = list(set(files_on_host) - set(files_in_s3) )
        logger.debug(f'Found {len(files_for_upload)} files for upload')
        return files_for_upload


    def _update_filename_with_date_time(self, file_with_path):
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
        

    def upload_file_list(self, files: dict = None):
        """
            given a list of files use _compare_bucket_contents_with_tracked_files()
            to get files already in S3, then upload any that are not in S3 already.
        """
        keys_for_upload = self._compare_bucket_contents_with_tracked_files(files.keys())
        files_for_upload = [v for k, v in files.items() if k in keys_for_upload]
        logger.debug(f'Deduplicated files. Next attempt to upload {len(files_for_upload)}')
        failed_uploads = []
        console = Console()
        test_count = 5
        with console.status("[bold green] Uploading files...") as status:
            try:
                for v in files_for_upload:
                    if "/var" in v:
                        # for some reason these fail
                        continue
                    logger.debug(f'attempting to upload {v}')
                    #TODO: remove
                    # object_name = os.path.basename(v)
                    object_name = self._update_filename_with_date_time(v)
                    response = self.bucket.upload_file(v, object_name)
                    print(response)
            except FileNotFoundError as e:
                failed_uploads.append(v)
            except ClientError as e:
                logger.error(f'received a client error trying to upload file {f} to bucket: {e}')
                raise e
        if len(failed_uploads):
            logger.error(f'Failed to upload {len(failed_uploads)} files to BUCKET {self.s3_bucket_name}')
        logger.info(f'Uploaded {len(files_for_upload)} files to BUCKET {self.s3_bucket_name}')
        self._upload_log()
        return True
    
    def _upload_log(self, log_name='ubiquiti_scripts_backup.log'):
        try:
            response = self.bucket.upload_file(v, object_name)
        except FileNotFoundError as e:
                failed_uploads.append(v)
        except ClientError as e:
            logger.error(f'received a client error trying to upload file {f} to bucket: {e}')
            raise e
        logger.debug(f'uploaded log file {log_name}')