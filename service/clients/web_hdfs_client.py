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

import json
import os
import random
import time
import io
import shutil
import tempfile
import urllib3
import tarfile

from flask import Response, send_file
from hdfs import InsecureClient

from service.exception.exceptions import ServiceError, ObjectNotFoundError, BadRequestError
from service.utils.environment import Environment
from service.utils.rest_util import RestUtil
from service.utils.sw_logger import SwLogger
from service.utils.sw_session_manager import SwSessionManager

logger = SwLogger(__name__)


class WebHdfsClient:
    """Client class to manage connectivity to HDFS files"""

    def __init__(self):
        self.url = Environment().get_web_hdfs_url() + "/webhdfs/v1/"
        self.auth = None
        if Environment().is_kerberos_enabled():
            from requests_kerberos import HTTPKerberosAuth, REQUIRED
            self.auth = HTTPKerberosAuth(mutual_authentication=REQUIRED, sanitize_mutual_error_response=False)

    def delete_file(self, file_name_with_path):
        """
        Deletes a file from HDFS identified by the path

        Keyword arguments:
            file_name_with_path {str} -- Name of the file identified with a path

        Returns:
             response -- Http method response
        """
        delete_file_url = self.url + file_name_with_path + "?op=DELETE"

        if os.path.splitext(file_name_with_path)[-1] == "":
            delete_file_url = delete_file_url + "&recursive=true"

        response = RestUtil.request_with_retry().delete(delete_file_url, auth=self.auth)

        if not response.ok:
            raise ServiceError("Attempt to delete file {0} failed with {1} and {2}.".format(file_name_with_path, response.status_code, response.reason))

        return response

    def download_file(self, file_name_with_path):
        """
        Downloads a file from HDFS location identified by the path

        Keyword arguments:
            file_name_with_path {str} -- Name of the file identified with a path

        Returns:
             response -- Default Flask response object with file content and appropriate headers set
        """
        file_name_with_path = self._get_actual_download_file_path(file_name_with_path)

        open_file_url = self.url + file_name_with_path + "?op=OPEN"

        response = RestUtil.request_with_retry().get(open_file_url, auth=self.auth, allow_redirects=False)
        if response.status_code != 307:
            if response.status_code == 404:
                raise ObjectNotFoundError("File {} not found.".format(file_name_with_path))
            raise ServiceError(
                "Attempt to open file {0} failed with {1} and {2}.".format(file_name_with_path, response.status_code,
                                                                           response.reason))

        file_download_url = None
        if response.headers is not None:
            file_download_url = response.headers["Location"]

        if file_download_url is not None:
            res = RestUtil.request_with_retry().get(file_download_url, auth=self.auth, stream=True)
            if not response.ok:
                raise ServiceError(
                    "Attempt to download file {0} failed with {1} and {2}.".format(file_name_with_path, response.status_code, response.reason))

            response = Response(res.content, headers=dict(res.headers))
            response.headers['Content-Type'] = 'application/octet-stream'
            response.headers['Content-Disposition'] = 'attachment;filename="{}"'.format(file_name_with_path.split("/")[-1])

            return response

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
        # The following lines (114-123) are a temporary fix until we have the right change made in the
        # ibm-wos-utils module to handle upload of this specific file only when its not found
        # in the HDFS location
        if "main_job.py" in file_name_with_path:
            check_file_status_url = self.url + file_name_with_path + "?op=LISTSTATUS"
            response = RestUtil.request_with_retry().get(check_file_status_url, auth=self.auth)
            if response.status_code == 200:
                logger.log_warning("File already exists.. Skipping upload...")
                response = {
                    "status": "finished",
                    "location": file_name_with_path
                }
                return response

        create_file_url = self.url + file_name_with_path + "?op=CREATE"
        if overwrite:
            create_file_url = create_file_url + "&overwrite=true"

        response = RestUtil.request_with_retry().put(create_file_url, auth=self.auth, allow_redirects=False)
        if response.status_code != 307:
            raise ServiceError(
                "Attempt to create file {0} failed with {1} and {2}.".format(file_name_with_path, response.status_code,
                                                                             response.reason))

        file_write_url = None
        if response.headers is not None:
            file_write_url = response.headers["Location"]

        if file_write_url is not None:
            response = RestUtil.request_with_retry().put(file_write_url, auth=self.auth, data=data)

            retry_attempt = 0
            sleep_factor = random.randint(1, 5)
            # If the file upload fails with 404, during multiple parallel requests trying to upload the same file,
            # attempting retry up-to 5 times with a random start sleep time ranging between 1 and 5 seconds
            # and a back-off factor of 1.5
            if response.status_code == 404:
                while retry_attempt < 5:
                    sleep_factor = sleep_factor * 1.5
                    time.sleep(sleep_factor)
                    retry_attempt += 1
                    logger.log_info("Re-attempt {} of file {} upload.".format(retry_attempt, file_name_with_path))
                    actual_response = RestUtil.request_with_retry().put(create_file_url, auth=self.auth, allow_redirects=False)
                    if actual_response.headers is not None:
                        file_url = actual_response.headers["Location"]
                        response = RestUtil.request_with_retry().put(file_url, auth=self.auth, data=data)

            if not response.ok:
                raise ServiceError(
                    "Attempt to write to file {0} failed with {1} and {2}.".format(file_name_with_path,
                                                                                   response.status_code,
                                                                                   response.reason))

            check_file_status_url = self.url + file_name_with_path + "?op=LISTSTTAUS"
            response = RestUtil.request_with_retry().get(check_file_status_url, auth=self.auth)
            if response.status_code == 404:
                raise ServiceError("File {} not found".format(file_name_with_path))

            response = {
                "status": "finished",
                "location": file_name_with_path
            }
            return response

    def _get_actual_download_file_path(self, file_name_with_path):

        download_file_path = None

        list_status_url = self.url + file_name_with_path + "?op=LISTSTATUS"

        response = RestUtil.request_with_retry().get(list_status_url, auth=self.auth)

        if not response.ok:
            if response.status_code == 404:
                raise ObjectNotFoundError("File {} not found.".format(file_name_with_path))
            raise ServiceError(
                "Attempt to open file {0} failed with {1} and {2}.".format(file_name_with_path, response.status_code,
                                                                           response.reason))

        list_status_response = json.loads(response.text)
        if list_status_response is not None and list_status_response.get("FileStatuses") is not None:
            files_statuses = list_status_response.get("FileStatuses")
            if files_statuses.get("FileStatus") is not None:
                file_status_list = files_statuses.get("FileStatus")

                if len(file_status_list) > 1:
                    return [file_name_with_path]
                    
                path_suffix = file_status_list[0]["pathSuffix"]
                if len(path_suffix) > 0:
                    if file_status_list[0]["type"] == "DIRECTORY":
                        download_file_path = self._get_actual_download_file_path(
                            file_name_with_path + "/" + path_suffix)
                    elif file_status_list[0]["type"] == "FILE":
                        download_file_path = file_name_with_path + "/" + path_suffix
                else:
                    download_file_path = file_name_with_path

        return download_file_path

    def download_directory(self, directory_url):
        '''Downloads directory from remote HDFS to local, archives it and
        returns the zip of the directory'''
        logger.log_info("Downloading the directory {0} ".format(directory_url))
        # Remove the base url from the absolute directory path provided as parameter
        # For example, if the absolute path is hdfs://alpha:9000/configuration/12345/drift,
        # the below statement will return /configuration/12345/drift
        directory_name_with_path = urllib3.util.parse_url(directory_url).path
        directory_name = os.path.split(directory_name_with_path)[1]
        web_hdfs_url = Environment().get_web_hdfs_url()
        session = SwSessionManager().get_session()
        user_name = session.get_username()
        client = InsecureClient(web_hdfs_url, user_name)
        try:
            with tempfile.TemporaryDirectory() as temp:
                client.download(hdfs_path=directory_name_with_path, local_path=temp, n_threads=5)
                tmp_archive = os.path.join(temp)
                data = io.BytesIO()
                with open(shutil.make_archive(tmp_archive, 'gztar', temp), "rb") as output_data:
                    data.write(output_data.read())
                data.seek(0)
            return send_file(data, as_attachment=True, attachment_filename=directory_name+".tar.gz")
        except Exception as e:
            raise ServiceError("Downloading the folder from HDFS failed with the error: {0}".format(str(e)))

    def upload_directory(self, directory_path, archive_directory_data):
        '''Untars the archive_directory_data provided as input,
        and uploads all the contents of the tar to the directory path
        specified on HDFS.
        '''
        logger.log_info("Uploading the directory to HDFS")
        web_hdfs_url = Environment().get_web_hdfs_url()
        hdfs_file_base_url = Environment().get_hdfs_file_base_url()
        session = SwSessionManager().get_session()
        user_name = session.get_username()
        client = InsecureClient(web_hdfs_url, user_name)
        directory_name_with_path = "/" + directory_path
        directory_name = os.path.split(directory_path)[1]
        try:
            with tempfile.TemporaryDirectory() as temp:
                local_dir_path = temp + "/" + directory_name +".tar.gz"
                with open(local_dir_path, "wb") as dir_archive:
                    dir_archive.write(archive_directory_data)
                with tarfile.open(local_dir_path, "r:gz") as tar:
                    tar.extractall(temp)
                os.remove(local_dir_path)
                response = client.upload(hdfs_path=directory_name_with_path, local_path=temp)
                logger.log_info("Successfully uploaded the directory {0} to HDFS".format(response))
            return hdfs_file_base_url + directory_name_with_path

        except Exception as e:
            raise ServiceError("Uploading the directory to HDFS failed with the error: {0}".format(str(e)))
                
    def delete_directory(self, directory_url):
        web_hdfs_url = Environment().get_web_hdfs_url()
        session = SwSessionManager().get_session()
        user_name = session.get_username()
        client = InsecureClient(web_hdfs_url, user_name)
        try:
            directory_name_with_path = urllib3.util.parse_url(directory_url).path
            logger.log_info("Deleting the directory {}".format(directory_name_with_path))
            response = client.delete(directory_name_with_path, recursive=True)
            if not response:
                raise ServiceError("Directory {0} doesn't exist".format(directory_name_with_path))
            return

        except Exception as e:
            raise ServiceError("Deleting the folder from HDFS failed with the error: {0}".format(str(e)))

