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

from flask import request
from flask_restplus import Namespace, Resource

from service.core.files_provider import FilesProvider
from service.resources.entity.sw_model import SwModel
from service.utils.sw_logger import SwLogger

ns = Namespace("Files")
swagger_model = SwModel(ns)
logger = SwLogger(__name__)


@ns.route("/files")
class Files(Resource):

    @ns.doc(id="post", description="Uploads file to HDFS.")
    @ns.param(name="file", description="Name of the file to be uploaded with path.", _in="query", required=True, example="arun/testing/first_spark_job.py")
    @ns.param(name="overwrite", description="Flag to overwrite the file if already exists.", _in="query", required=False)
    @ns.response(201, "File uploaded successfully.", swagger_model.upload_file_response_model)
    @ns.response(401, "Unauthorized", swagger_model.error_container)
    @ns.response(500, "Internal Server Error", swagger_model.error_container)
    def put(self):

        response_status_code = 201

        # grab all headers
        file_name = request.args.get("file")
        overwrite = request.args.get("overwrite")

        overwrite_flag = False
        if overwrite is not None and overwrite in ["TRUE", "true", "True"]:
            overwrite_flag = True

        response_content = FilesProvider().upload_file(file_name_with_path=file_name, data=request.data, overwrite=overwrite_flag)
        return response_content, response_status_code

    @ns.doc(id="get", description="Downloads the file from HDFS.")
    @ns.produces(["application/octet-stream"])
    @ns.param(name="file", description="Name of the file or folder with path that should be downloaded from the remote HDFS. In case of folder path, download supported only if folder has single part file inside it.", _in="query", required=True, example="arun/testing/first_spark_job.py")
    @ns.response(200, "File downloaded successfully.")
    @ns.response(401, "Unauthorized", swagger_model.error_container)
    @ns.response(500, "Internal Server Error", swagger_model.error_container)
    def get(self):

        # grab all headers
        file_name = request.args.get("file")

        response_content = FilesProvider().download_file(file_name_with_path=file_name)
        return response_content

    @ns.doc(id="delete", description="Deletes the file or folder from HDFS.")
    @ns.param(name="file", description="Name of the file or folder with path that should be deleted from the remote HDFS.", _in="query", required=True, example="arun/testing/first_spark_job.py")
    @ns.response(200, "File deleted successfully.")
    @ns.response(401, "Unauthorized", swagger_model.error_container)
    @ns.response(500, "Internal Server Error", swagger_model.error_container)
    def delete(self):

        response_status_code = 200

        # grab all headers
        file_name = request.args.get("file")

        response_content = FilesProvider().delete_file(file_name_with_path=file_name)
        return response_content, response_status_code
