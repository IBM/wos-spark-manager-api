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
from service.resources.entity.run_job_request import RunJobRequestSchema
from service.core.jobs_provider import JobsProvider
from service.resources.entity.sw_model import SwModel
from service.utils.constants import SYNC_JOB_MAX_WAIT_TIME

from service.utils.sw_logger import SwLogger

ns = Namespace("Jobs")
swagger_model = SwModel(ns)
logger = SwLogger(__name__)


@ns.route("/jobs")
class Jobs(Resource):

    @ns.expect(swagger_model.run_job_request_model, validate=True)
    @ns.doc(id="post", description="Submit and run a job.", body=swagger_model.run_job_request_model)
    @ns.param(name="background_mode", description="Run the job in background mode. Defaults to true", _in="query", required=False)
    @ns.param(name="timeout", description="The timeout when the job is not running in background mode. Applicable only when background_mode=False", _in="query", required=False)
    @ns.response(200, "Job finished successfully.", swagger_model.run_job_response_model)
    @ns.response(202, "Job accepted successfully.", swagger_model.run_job_response_model)
    @ns.response(400, "Bad Request", swagger_model.error_container)
    @ns.response(401, "Unauthorized", swagger_model.error_container)
    @ns.response(500, "Internal Server Error", swagger_model.error_container)
    def post(self):

        response_status_code = 200

        background_mode_flag = True
        background_mode = request.args.get("background_mode")
        if background_mode is not None and background_mode.lower() == "false":
            background_mode_flag = False
            response_status_code = 202
        timeout = request.args.get("timeout") or SYNC_JOB_MAX_WAIT_TIME

        request_json = {}
        if not request.data:
            pass
        else:
            request_json = request.get_json()
        logger.log_debug(str(request_json))

        run_request_payload = RunJobRequestSchema().load(request_json).data
        response_content = JobsProvider().run_job(
            run_request_payload, background_mode_flag, timeout)

        return response_content, response_status_code


@ns.route("/jobs/<string:job_id>/status")
class GetJobStatus(Resource):

    @ns.doc(id="get", description="Fetches the job status for the job identified by the job identifier.")
    @ns.response(200, "Job status fetched successfully.", swagger_model.get_job_status_response_model)
    @ns.response(401, "Unauthorized", swagger_model.error_container)
    @ns.response(404, "Object not found", swagger_model.error_container)
    @ns.response(500, "Internal Server Error", swagger_model.error_container)
    def get(self, job_id):

        response_status_code = 200
        response_content = JobsProvider().get_job_status(job_id)
        return response_content, response_status_code


@ns.route("/jobs/<string:job_id>/logs")
class JobLogs(Resource):

    @ns.doc(id="get", description="Fetches the logs for the jobs identified by the job identifier.")
    @ns.param(name="size", description="Max number of log lines to return", _in="query", required=False)
    @ns.response(200, "Job logs fetched successfully.", swagger_model.get_job_logs_response_model)
    @ns.response(401, "Unauthorized", swagger_model.error_container)
    @ns.response(404, "Object not found", swagger_model.error_container)
    @ns.response(500, "Internal Server Error", swagger_model.error_container)
    def get(self, job_id):

        response_status_code = 200

        size = request.args.get("size")
        number_of_lines = 0
        if size is not None:
            number_of_lines = int(size)

        response_content = JobsProvider().get_job_logs(job_id, number_of_lines)
        return response_content, response_status_code
