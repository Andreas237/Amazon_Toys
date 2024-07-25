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
    f_list = vm.find_video_files(extension="*.conf",root_directory="/home/ace/work/amazons/")
    print(f'run completed')