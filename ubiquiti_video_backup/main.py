import logging
import sys

from video_file_manager import VideoManager
from s3_manager import S3Manager


logger = logging.getLogger('ui_backups')
logger.setLevel(logging.DEBUG)

logFormatter = logging.Formatter('%(asctime)s\t[%(levelname)s]\t%(filename)s|%(module)s|%(funcName)s\t%(message)s')

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler(filename='ubiquiti_scripts_backup.log')
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.DEBUG)

logger.addHandler(consoleHandler)
logger.addHandler(fileHandler)



if __name__ == "__main__":
    vm = VideoManager()
    files_on_host_dict = vm.find_video_files(extension="*.sh",root_directory="/home/ace/work/amazons/")
    s3m = S3Manager()
    s3m._compare_bucket_contents_with_tracked_files(files_on_host=files_on_host_dict.keys())
    # logging.debug(f'File for upload: {file_for_upload}')
    # s3m.upload_file_list(files=file_for_upload)
    logging.debug(f's3m upload complete!')
    print(f'run completed')
