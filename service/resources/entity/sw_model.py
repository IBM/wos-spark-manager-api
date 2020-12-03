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

from flask_restplus import fields, Namespace


class SwModel:

    def __init__(self, ns: Namespace):
        self.ns = ns

        self.run_job_request_model = self.ns.model("RunJobRequest", {
            "args": fields.List(fields.String(), description="Command line arguments for the application.", example=[]),
            "file": fields.String(description="File containing the application to execute.", example="arun/testing/first_spark_job.py"),
            "pyFiles": fields.List(fields.String(), description="Python files to be used in this session.", example=["arun/testing/bias.zip"]),
            "proxyUser": fields.String(description="User to impersonate when running the job"),
            "className": fields.String(description="Application Java/Spark main class"),
            "jars": fields.List(fields.String(), description="jars to be used in this session"),
            "files": fields.List(fields.String(), description="files to be used in this session"),
            "driverMemory": fields.String(description="Amount of memory to use for the driver process"),
            "driverCores": fields.Integer(decription="Number of cores to use for the driver process"),
            "executorMemory": fields.String(description="Amount of memory to use per executor process"),
            "executorCores": fields.Integer(description="Number of cores to use for each executor"),
            "numExecutors": fields.Integer(description="Number of executors to launch for this session"),
            "archives": fields.List(fields.String(), description="Archives to be used in this session"),
            "queue": fields.String(description="The name of the YARN queue to which submitted"),
            "name": fields.String(description="The name of this session"),
            "conf": fields.Raw(description="Spark configuration properties")
        })

        self.run_job_response_model = self.ns.model("RunJobResponse", {
            "id": fields.Integer(description="The job id."),
            "state": fields.String(description="The job state."),
            "appId": fields.String(description="The application id of this job.")
        })

        self.get_job_status_response_model = self.ns.model("GetJobStatusResponse", {
            "id": fields.Integer(description="The job id."),
            "state": fields.String(description="The job state."),
            "appId": fields.String(description="The application id of this job.")
        })

        self.get_job_logs_response_model = self.ns.model("GetJobLogsResponse", {
            "id": fields.Integer(description="The job id."),
            "from": fields.Integer(description="Offset from start of log."),
            "size": fields.Integer(description="Number of log lines."),
            "log": fields.List(fields.String(), description="The log lines.")
        })

        self.upload_file_response_model = self.ns.model("UploadFilesResponse", {
            "status": fields.String(description="Status of the upload operation."),
            "location": fields.String(description="Relative path of the file uploaded.")
        })

        self.error_model = self.ns.model("ErrorModel", {
            "message": fields.String(description="The message explaining the error and a possible solution.",
                                     example="Error occurred")
        })

        self.error_container = self.ns.model("ErrorContainer", {
            "errors": fields.List(fields.Nested(self.error_model), description="The list of error objects.")
        })
