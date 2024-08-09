import boto3
from botocore.exceptions import ClientError
from fnmatch import fnmatch
import logging
import multiprocessing
import os

from rich.console import Console

logger = logging.getLogger('ui_backups')
#TODO: remove
# if logging.getLevelName(logger.level) == "DEBUG":
#     boto3.set_stream_logger('')

def take(n:int , iterable) -> list:
         from itertools import islice
         return list(islice(iterable, n))

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
        parts = file_with_path.split('/')
        idx = parts.index('video')
        filename = parts[idx + 1] + parts[idx + 2] + parts[idx + 3] + "_" + parts[idx + 4]
        return filename

    def _files_list_to_dict(self, files: list[str], files_dict: dict[str,str]) -> None:
        """
            Given a list of filepaths as strings, update files_dict(dict) with the
            a key of filepath and value of self._get_filename_with_date_time()
        """
        for f in files:
            if '/var' in f:
                continue
            # files_dict[f] = self._get_filename_with_date_time(f)
            files_dict[self._get_filename_with_date_time(f)] = f
        return files_dict
    
    def _mp_files_list_to_dict(self, files: list[str] = None) -> dict[str,str]:
        """
            Create a dictionary of the files on host in the format needed for upload:
            {filepath: desire file name}
        """
        ctx = multiprocessing.get_context()
        logger.debug(f'processing a list of {len(files)} into a dictionary.')
        manager = multiprocessing.Manager()
        files_dict = manager.dict()
        cpus = multiprocessing.cpu_count() // 2
        logger.debug(f'multiprocessing with {cpus} CPU cores')
        with ctx.Pool(processes=cpus) as p:
            # files_dict = p.apply(self._files_list_to_dict, args=(files, files_dict,))
            # p.join()
            p = multiprocessing.Process(target=self._files_list_to_dict, args=(files, files_dict,))
            p.start()
            p.join()
        logger.debug(f'processing to dict of {len(files_dict)} records complete!')
        return files_dict

    def _reduce_dict_of_files_for_upload(self, files_on_host_dict: dict = None) -> dict[str, str]:
            """
                Removes keys that exist in the target bucket from the dict of files on host
                returns the resulting dict.
            """
            logger.debug(f'before removing keys: {len(files_on_host_dict)}')
            bucket_contents = self._get_bucket_contents()
            keys_in_s3 = [i.key for i in bucket_contents]
            for k in keys_in_s3: files_on_host_dict.pop(k, None)
            logger.debug(f'after removing keys: {len(files_on_host_dict)}')

    def _upload_log(self, log_name='ubiquiti_scripts_backup.log'):
        try:
            response = self.bucket.upload_file(log_name, os.path.basename(log_name))
        except FileNotFoundError as e:
                logger.error(f'Missing log file!  Failed to upload log file with exception {e}')
        except ClientError as e:
            logger.error(f'received a client error trying to upload file {f} to bucket: {e}')
            raise e
        logger.debug(f'uploaded log file {log_name}')


    def upload_file_list(self, files: list = None):
        """
            given a list of files, remove keys for files already in S3.
            Upload any remaining files to s3.
        """
        files = self._mp_files_list_to_dict(files)
        self._reduce_dict_of_files_for_upload(files)
        logging.error(f'slice of dict: {take(5, files)}')
        logger.debug(f'Deduplicated files. Next attempt to upload {len(files)}')
        failed_uploads = []
        console = Console()
        with console.status("[bold green] Uploading files...") as status:
            try:
                for k, v in files.items():
                    if "/var" in v:
                        continue
                    response = self.bucket.upload_file(v, k)
                    logger.debug(f'response for file {k} {response}')
            except FileNotFoundError as e:
                logger.error(f'FileNotFoundError-- failed to upload file {v} to bucket: {e}')
                failed_uploads.append(v)
            except ClientError as e:
                logger.error(f'ClientError-- failed to upload file {v} to bucket: {e}')
                failed_uploads.append(v)
            except S3UploadFailedError as e:
                logger.error(f'S3UploadFailedError-- failed to upload file {v} to bucket: {e}')
                failed_uploads.append(v)
        if len(failed_uploads):
            logger.error(f'Failed to upload {len(failed_uploads)} files to BUCKET {self.s3_bucket_name}')
        logger.info(f'Uploaded {len(files) - len(failed_uploads)} files to BUCKET {self.s3_bucket_name}')
        self._upload_log()
        return True