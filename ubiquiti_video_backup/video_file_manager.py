from fnmatch import fnmatch
import logging
import os
from os.path import join



class VideoManager:
    
    logger = logging.getLogger('ui_backups')
    
    def __init__(self,):
        print(self.logger.hasHandlers())
        self.files = []
        self.file_dict = {}
        self.log_file = None
        for name in os.listdir('./'):
            if fnmatch(name, "*.log"):
                self.log_file = name
        if self.log_file == None:
            self.log_file = str(os.getenv('hostname') + 'ubiquti_script_logging.log')
        self.logger.debug(f'finished setting up')

    def find_video_files(self,extension="*.ubv", root_directory="/") -> None:
        
        for root, dirs, fs in os.walk(root_directory):
            for name in fs:
                if fnmatch(name, extension):
                    self.files.append(join(root,name))
                    self.file_dict[name] = join(root,name)
        self.logger.debug(f'Search root directory {root_directory} for files ending in {extension}; found {len(self.file_dict)} files')
        self.logger.debug(f'file dict:\n{self.file_dict}')

    def diff_file_lists(self, files_in_s3, files_on_host) -> None:
        """
            Files exist in S3 and on the system.  Diff the files on the system with those in S3 to determine which to upload
        """
        return list(set(files_on_host) - set(files_in_s3))


if __name__ == "__main__":
    vm = VideoManager()
    f_list = vm.find_video_files(extension="*.py",root_directory="/home/ace/work/amazons/")
    # print(f_list)