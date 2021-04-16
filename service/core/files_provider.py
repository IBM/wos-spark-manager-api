# ----------------------------------------------------------------------------------------------------
# (C) Copyright IBM Corp. 2020.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
# ----------------------------------------------------------------------------------------------------
import time
from string import Template

from service.clients.web_hdfs_client import WebHdfsClient
from service.exception.exceptions import ServiceError
from service.utils.environment import Environment
from service.utils.sw_logger import SwLogger

logger = SwLogger(__name__)


class FilesProvider:
    """Delegator class that delegates the call to the appropriate client method to perform various file operations
    using WebHDFS"""

    def __init__(self):
        self.client = WebHdfsClient()

    def upload_file(self, file_name_with_path, data, overwrite=False):
        """
        Uploads file to a HDFS location identified by the path

        Keyword arguments:
            file_name_with_path {str} -- Name of the file identified with a path
            data {bytearray} -- Byte array representation of the file
            overwrite {bool} -- Flag indicating of the file should be overwritten

        Returns:
             response {dict} -- Dictionary denoting the status of the upload operation and the relative location of the file.
        """
        response = None
        try:
            response = self.client.upload_file(self.__update_absolute_hdfs_file_path(file_name_with_path), data, overwrite)
        except Exception as ex:
            logger.log_exception("File upload operation failed", exc_info=True)
            if isinstance(ex, ServiceError):
                raise ex
        return response

    def download_file(self, file_name_with_path):
        """
        Downloads a file from HDFS location identified by the path

        Keyword arguments:
            file_name_with_path {str} -- Name of the file identified with a path

        Returns:
             response -- Default Flask response object with file content and appropriate headers set
        """
        response = None
        try:
            response = self.client.download_file(self.__update_absolute_hdfs_file_path(file_name_with_path))
        except Exception as ex:
            logger.log_exception("File download operation failed", exc_info=True)
            if isinstance(ex, ServiceError):
                raise ex
        return response

    def download_directory(self, directory_path):
        """
        Downloads a file from HDFS location identified by the path

        Keyword arguments:
            directory_path {str} -- Name of the directory identified with a path

        Returns:
             response -- Default Flask response object with archived directory content and appropriate headers set
        """
        response = None
        try:
            start_time = time.time()
            response = self.client.download_directory(directory_path)
            end_time = time.time()
            logger.log_info("Time taken to download the directory {0} is {1}".format(
                    directory_path, str(end_time - start_time)
                ))
        except Exception as ex:
            logger.log_exception("Directory download operation failed", exc_info=True)
            if isinstance(ex, ServiceError):
                raise ex
        return response

    def delete_file(self, file_name_with_path):
        """
        Deletes a file from HDFS identified by the path

        Keyword arguments:
            file_name_with_path {str} -- Name of the file identified with a path

        Returns:
             response -- Http method response
        """
        response = None
        try:
            response = self.client.delete_file(self.__update_absolute_hdfs_file_path(file_name_with_path))
        except Exception as ex:
            logger.log_exception("File delete operation failed", exc_info=True)
            if isinstance(ex, ServiceError):
                raise ex
        return response

    def delete_directory(self, directory_path):
        """
        Deletes a directory and it's contents recursively from HDFS identified by the path

        Keyword arguments:
            directory_path {str} -- Name of the directory identified with a path

        Returns:
             response -- Http method response
        """
        response = None
        try:
            self.client.delete_directory(directory_path)
            
        except Exception as ex:
            logger.log_exception("Directory delete operation failed", exc_info=True)
            if isinstance(ex, ServiceError):
                raise ex
        return response

    
    def upload_directory(self, hdfs_directory_path, archive_directory_data):
        """
        Upplaods a directory to HDFS identified by the path

        Keyword arguments:
            hdfs_directory_path {str} -- Name of the directory identified with a path
            archive_directory_data {binary data} - Directory content in tar.gz format

        Returns:
             response -- Http method response
        """
        response = None
        start_time = time.time()
        try:
            absolute_directory_path = self.__update_absolute_hdfs_file_path(hdfs_directory_path)
            response = self.client.upload_directory(absolute_directory_path, archive_directory_data)
            end_time = time.time()
            logger.log_info("Time taken to upload the directory {0} is {1}".format(
                    hdfs_directory_path, str(end_time - start_time)))
        except Exception as ex:
            logger.log_exception("Directory upload operation failed", exc_info=True)
            if isinstance(ex, ServiceError):
                raise ex
        return response

    

    @staticmethod
    def __update_absolute_hdfs_file_path(file_name_with_path):

        param_dict = {
            "hdfs": Environment().get_base_hdfs_location()
        }
        if file_name_with_path is not None:
            if file_name_with_path.startswith("$hdfs"):
                replaced_file_path = Template(file_name_with_path)
                replaced_file_path = replaced_file_path.substitute(param_dict)
                return replaced_file_path
            elif file_name_with_path.startswith("/"):
                file_name_with_path = Environment().get_base_hdfs_location() + "/" + file_name_with_path[1:]
            else:
                file_name_with_path = Environment().get_base_hdfs_location() + "/" + file_name_with_path

        return file_name_with_path
