from fnmatch import fnmatch
import logging
import os
from os.path import join

from rich.console import Console

logger = logging.getLogger('ui_backups')

class VideoManager:
    
    def __init__(self,):
        print(logger.hasHandlers())
        self.files = []
        self.file_dict = {}
        self.log_file = None
        logger.debug(f'finished setting up')

    def find_video_files(self,extension="*.ubv", root_directory="/") -> dict[str,str]:
        console = Console()
        with console.status("[bold green] Searching for files...") as status:
            for root, dirs, fs in os.walk(root_directory):
                for name in fs:
                    if fnmatch(name, extension):
                        self.files.append(join(root,name))
                        self.file_dict[name] = join(root,name)
        logger.debug(f'Search root directory {root_directory} for files ending in {extension}; found {len(self.file_dict)} files')
        logger.debug(f'Found {len(self.file_dict)} files in search.')
        # logger.debug(f'file dict:\n{self.file_dict}')
        return self.file_dict

    def get_video_files_list(self,extension="*.ubv", root_directory="/") -> list[str]:
        if len(self.files) == 0:
            self.find_video_files(extension=extension, root_directory=root_directory)
        return self.files
    

if __name__ == "__main__":
    vm = VideoManager()
    vm.find_video_files(extension="*.sh", root_directory="/Users/ace/work/Amazon_Toys/ubiquiti_video_backup")