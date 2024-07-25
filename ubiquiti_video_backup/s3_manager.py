import boto3
from botocore.exceptions import ClientError
import logging
import os


logging.basicConfig(level=logging.DEBUG)


class S3Manager:
    S3_BUCKET_NAME = None

    def __init__(self,):


        self.S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME",
                                        default="udm_pro_video_backups_SloDown-5d1a2f64-718b-4f49-a45c-cce76e82d497")
        logging.debug(f'self bucket name: {self.S3_BUCKET_NAME}')
        self.s3_resource = boto3.resource("s3")


    def _create_s3_bucket(self,) -> bool:
        if not self._check_if_bucket_exists():
            try:
                bucket = self.s3_resource.create_bucket(
                    Bucket = self.S3_BUCKET_NAME,
                )
            except ClientError as e:
                logging.error(f'received a client error trying to create the bucket: {e}')
                raise e
        logging.debug(f'Create bucket with name {self.S3_BUCKET_NAME}')
        return True
    
    def _check_if_bucket_exists(self,) -> bool:
        for bucket in self.s3_resource.buckets.all():
            logging.debug(f'bucket name: {bucket.name}')
            if bucket.name == self.S3_BUCKET_NAME:
                return True
        return False
        
    def _get_bucket_contents(self,) -> list:
        contents = []
        try:
            contents = self.s3_resource.objects.all()
        except ClientError as e:
            logging.error(f'received a client error trying to access bucket contents: {e}')
            raise e
        logging.debug(f'list of items in {self.s3_resource}:\n{contents}')
        return contents

if __name__ == "__main__":
    s3m = S3Manager()
    print(s3m._check_if_bucket_exists())
    s3m._create_s3_bucket()
    s3m._get_bucket_contents()