from fnmatch import fnmatch
import logging
import os
from os.path import join

logging.basicConfig(level=logging.DEBUG)

class VideoManager:
    def find_video_files(self,extension="*.ubv", root_directory="/") -> None:
        files = []
        file_dict = {}
        for root, dirs, fs in os.walk(root_directory):
            for name in fs:
                if fnmatch(name, extension):
                    self.files.append(join(root,name))
                    self.file_dict[name] = join(root,name)
        logging.debug(f'Search root directory {root_directory} for files ending in {extension}; found {len(file_dict)} files')
        logging.debug(f'file dict:\n{file_dict}')

    def diff_file_lists(self, files_in_s3, files_on_host) -> None:
        """
            Files exist in S3 and on the system.  Diff the files on the system with those in S3 to determine which to upload
        """
        return list(set(files_on_host) - set(files_in_s3))


if __name__ == "__main__":
    vm = VideoManager()
    f_list = vm.find_video_files(extension="*.py",root_directory="/home/ace/work/amazons/")
    # print(f_list)