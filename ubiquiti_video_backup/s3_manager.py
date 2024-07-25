import boto3
from botocore.exceptions import ClientError
from fnmatch import fnmatch
import logging
import os

logger = logging.getLogger('ui_backups')


class S3Manager:
    S3_BUCKET_NAME = None
    BUCKET = None

    def __init__(self,):
        self.S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME",
                                        default="udm_pro_video_backups_SloDown-5d1a2f64-718b-4f49-a45c-cce76e82d497")
        logger.debug(f'self bucket name: {self.S3_BUCKET_NAME}')
        self.s3_resource = boto3.resource("s3")
        self.BUCKET = self.s3_resource.Bucket(self.S3_BUCKET_NAME)

        # make sure the log file exists and is uploaded!
        self.log_file = None
        for name in os.listdir('./'):
            if fnmatch(name, "*.log"):
                self.log_file = name
        if self.log_file == None:
            self.log_file = str(os.getenv('hostname') + 'ubiquti_script_logging.log')


    def _create_s3_bucket(self,) -> bool:
        if not self._check_if_bucket_exists():
            try:
                bucket = self.s3_resource.create_bucket(
                    Bucket = self.S3_BUCKET_NAME,
                )
            except ClientError as e:
                logging.error(f'received a client error trying to create the bucket: {e}')
                raise e
        logger.debug(f'Create bucket with name {self.S3_BUCKET_NAME}')
        return True


    def _check_if_bucket_exists(self,) -> bool:
        for bucket in self.s3_resource.buckets.all():
            logger.debug(f'bucket name: {bucket.name}')
            if bucket.name == self.S3_BUCKET_NAME:
                return True
        return False
        
    
    def _get_bucket_contents(self, prefix=None) -> list:
        contents = None
        try:
            if not prefix:
                contents = list(self.BUCKET.objects.all())
            else:
                contents = list(self.BUCKET.objects.filter(Prefix=prefix))
        except ClientError as e:
            logger.error(f'received a client error trying to access bucket contents: {e}')
            raise e
        logger.debug(f'list of items in {self.s3_resource}:\n{contents}')
        return contents


    def _compare_bucket_contents_with_tracked_files(self,) -> list:
        """
            Get the list of files matching the extensions. Return a list of files for upload
        """
        files_in_s3 = self._get_bucket_contents()
        files_on_system = ['test_file.txt']

        files_for_upload = list(set(files_on_system) - set(files_in_s3) )
        logging.debug(f'files to upload: {files_for_upload}')



if __name__ == "__main__":
    s3m = S3Manager()
    # print(s3m._check_if_bucket_exists())
    # s3m._create_s3_bucket()
    s3m._get_bucket_contents()