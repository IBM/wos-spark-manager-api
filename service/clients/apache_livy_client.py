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

import datetime
import time
from time import sleep

from service.utils import constants
from service.utils.environment import Environment
from service.utils.rest_util import RestUtil
from service.exception.exceptions import ServiceError, ObjectNotFoundError


class LivyClient:
    """Client class to interact with Remote Spark using Livy Rest API"""

    def __init__(self):
        self.url = Environment().get_spark_livy_url()
        self.auth = None
        if Environment().is_kerberos_enabled():
            from requests_kerberos import HTTPKerberosAuth, REQUIRED
            self.auth = HTTPKerberosAuth(mutual_authentication=REQUIRED, sanitize_mutual_error_response=False)

    def run_batch_job(self, job_json, background=True):
        """
        Submits a job request using Livy batches endpoint

        Keyword arguments:
            job_json {str} -- Job request payload
            background {bool} -- Flag indicating if the method should wait until the job finishes or return immediately after submitting the request.

        Returns:
             response {dict} -- Dictionary with job id, state and the application id.
        """
        job_url = "{}/batches".format(self.url)
        response = RestUtil.request_with_retry().post(
            url=job_url, json=job_json, headers={"Content-Type": "application/json"}, auth=self.auth)

        if not response.ok:
            raise ServiceError("Failed to run job. " + response.text)

        job_response = response.json()
        job_id = job_response.get("id")
        state = job_response.get("state")
        appId = job_response.get("appId")
        if background is False:
            start_time = time.time()
            elapsed_time = 0
            sleep_time = 15
            timeout = constants.SYNC_JOB_MAX_WAIT_TIME
            while state not in (constants.LIVY_JOB_FINISHED_STATE, constants.LIVY_JOB_FAILED_STATE,
                                constants.LIVY_JOB_DEAD_STATE, constants.LIVY_JOB_KILLED_STATE):
                if elapsed_time > timeout:
                    raise ServiceError("Job didn't come to Finished/Failed state in {} seconds. Current state is ".format(timeout, state))
                print("{}: Sleeping for {} seconds. Current state {}".format(datetime.datetime.now(), sleep_time, state))
                sleep(sleep_time)
                elapsed_time = time.time() - start_time
                status = self.get_job_status(job_id)
                state = status.get("state")
                appId = status.get("appId")

        response = {
            "id": job_id,
            "state": state,
            "appId": appId
        }

        return response

    def get_job_status(self, job_id):
        """
        Fetches the status of the batch job using Livy's batches endpoint

        Keyword arguments:
            job_id {str} -- Job identifier

        Returns:
             response {dict} -- Dictionary with job id, state and the application id.
        """
        job_url = "{}/batches/{}".format(self.url, job_id)

        response = RestUtil.request_with_retry().get(url=job_url, auth=self.auth)

        if not response.ok:
            if response.status_code == 404:
                raise ObjectNotFoundError("Job with id {} not found.".format(job_id))

            raise ServiceError("Failed to get jobs state. " + response.text)

        job_response = response.json()
        response = {
            "id": job_response.get("id"),
            "state": job_response.get("state"),
            "appId": job_response.get("appId")

        }

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
        job_logs_url = "{}/batches/{}/logs".format(self.url, job_id)

        if size is not None and size > 0:
            job_logs_url = job_logs_url + "?size={}".format(size)

        response = RestUtil.request_with_retry().get(url=job_logs_url, auth=self.auth)

        if not response.ok:
            if response.status_code == 404:
                raise ObjectNotFoundError("Job with id {} not found.".format(job_id))

            raise ServiceError("Failed to get job logs. " + response.text)

        return response
