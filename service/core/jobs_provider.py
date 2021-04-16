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
from string import Template

from service.clients.apache_livy_client import LivyClient
from service.exception.exceptions import BadRequestError
from service.resources.entity.run_job_request import RunJobRequest
from service.utils.environment import Environment
from service.utils.sw_logger import SwLogger
from service.utils.sw_session_manager import SwSessionManager
from service.utils.constants import SYNC_JOB_MAX_WAIT_TIME

logger = SwLogger(__name__)


class JobsProvider:
    """Delegator class that delegates the call to the appropriate client method to perform various job related
    operations using Livy"""

    def __init__(self):
        if Environment().get_base_hdfs_location():
            self.file_path_prefix = Environment().get_hdfs_file_base_url() + "/" + \
                Environment().get_base_hdfs_location()
        else:
            self.file_path_prefix = Environment().get_hdfs_file_base_url()
        self.client = LivyClient()

    def run_job(self, run_request: RunJobRequest, background=True, timeout=SYNC_JOB_MAX_WAIT_TIME):
        """
        Submits a job request using Livy batches endpoint

        Keyword arguments:
            run_request {RunJobRequest} -- Run job request
            background {bool} -- Flag indicating if the method should wait until the job finishes or return immediately after submitting the request.

        Returns:
             response {dict} -- Dictionary with job id, state and the application id.
        """
        run_request = self.__validate_run_request(run_request)
        run_request_json = self.__replace_hdfs_base_path_in_run_request_params(
            run_request)
        run_request_json = self.__add_additional_parameters_in_run_request(
            run_request_json)

        try:
            logger.log_info(
                "Dumping the run job payload >>>>>>>>>>>> {}".format(run_request_json))

            response = self.client.run_batch_job(
                run_request_json, background, timeout)
        except Exception as ex:
            logger.log_exception(
                "File upload operation failed.", exc_info=True)
            raise ex

        return response

    def get_job_status(self, job_id):
        """
        Fetches the status of the batch job using Livy's batches endpoint

        Keyword arguments:
            job_id {str} -- Job identifier

        Returns:
             response {dict} -- Dictionary with job id, state and the application id.
        """
        try:
            response = self.client.get_job_status(job_id)
        except Exception as ex:
            logger.log_exception("File to get job status.", exc_info=True)
            raise ex

        return response

    def get_job_logs(self, job_id, size):
        """
        Fetches the logs of the batch job using Livy's batches logs endpoint

        Keyword arguments:
            job_id {str} -- Job identifier
            size {int} -- Number of log lines to be returned

        Returns:
             response -- Http method response
        """
        try:
            response = self.client.get_job_logs(job_id, size)
        except Exception as ex:
            logger.log_exception("File to get job logs.", exc_info=True)
            raise ex

        return response

    @staticmethod
    def __validate_run_request(run_request: RunJobRequest):

        if run_request is None:
            raise BadRequestError("Run job request payload is missing")

        if run_request.file is None:
            raise BadRequestError(
                "'file' is missing in the run request payload")

        return run_request

    def __replace_hdfs_base_path_in_run_request_params(self, run_request: RunJobRequest):

        request_json = None
        updated_run_request = run_request
        if run_request is not None:
            param_dict = {
                "hdfs": self.file_path_prefix
            }
            request_json = Template(run_request.json())
            request_json = request_json.substitute(param_dict)

        def remove_nulls(d):
            return {k: v for k, v in d.items() if v is not None}

        if request_json is not None:
            return json.loads(request_json, object_hook=remove_nulls)

        return updated_run_request.get()

    def __update_absolute_file_path_in_run_request(self, run_request: RunJobRequest):

        if run_request.file is not None:
            run_request.file = self.file_path_prefix + "/" + run_request.file

        if run_request.pyFiles is not None:
            updated_pyfile_paths = []
            for pyfile in run_request.pyFiles:
                updated_pyfile_paths.append(
                    self.file_path_prefix + "/" + pyfile)
            run_request.pyFiles = updated_pyfile_paths

        return run_request

    @staticmethod
    def __add_additional_parameters_in_run_request(run_request_json):

        conf = run_request_json.get("conf")
        if conf is None:
            run_request_json["conf"] = {}

        # Add archives to the job payload
        if Environment().get_wos_env_archive_location() is not None:
            archives = run_request_json.get("archives")
            if archives is None:
                archives = []
            archives.append(Environment().get_wos_env_archive_location())
            run_request_json["archives"] = archives
            if Environment().get_wos_env_site_packages_path() is not None:
                run_request_json["conf"]["spark.yarn.appMasterEnv.PYTHONPATH"] = Environment(
                ).get_wos_env_site_packages_path()
                run_request_json["conf"]["spark.executorEnv.PYTHONPATH"] = Environment(
                ).get_wos_env_site_packages_path()

        if Environment().is_kerberos_enabled():
            session = SwSessionManager().get_session()
            run_request_json["conf"]["spark.yarn.principal"] = session.get_user_principal(
            )
            run_request_json["conf"]["spark.yarn.keytab"] = Environment(
            ).get_spark_yarn_keytab_file_path()

        return run_request_json
