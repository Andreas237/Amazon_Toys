import logging
import sys
from time import time

from video_file_manager import VideoManager
from s3_manager import S3Manager


logger = logging.getLogger('ui_backups')
logger.setLevel(logging.DEBUG)

logFormatter = logging.Formatter('\n\n%(asctime)s\t[%(levelname)s]\t%(filename)s|%(module)s|%(funcName)s|%(lineno)s\t%(message)s')

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler(filename='ubiquiti_scripts_backup.log')
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.DEBUG)

logger.addHandler(consoleHandler)
logger.addHandler(fileHandler)



if __name__ == "__main__":
    start_time = time()
    vm = VideoManager()
    files_list = vm.get_video_files_list()
    
    s3m = S3Manager()
    s3m.upload_file_list(files=files_list)
    end_time = time()

    logger.debug(f's3m upload complete! total execution time was {end_time - start_time} seconds.')